"""The matching saga (DESIGN.md §6.2, ADR-09). Used by the RabbitMQ consumer AND by
POST /match/{id}, so asynchronous and on-demand matching behave identically.

A request and a resource live on different shards and are owned by different services, so
"reserve the resource AND assign the request" cannot be one database transaction. Instead:

  0. RESUME / CLEAN UP              an earlier, interrupted attempt may have left a reservation
  1. read the request               (skip if it is no longer PENDING/UNMATCHED: idempotent consumer)
  2. find + rank candidates         (scatter-gather over the shards, then MatchingStrategy)
  3. RESERVE quantity on the best   (compare-and-set inside resource-service, cannot oversell)
  4. ASSIGN the request             (If-Match on the request version)
     if 4 definitely fails -> COMPENSATE: release the reservation from step 3

Outcome-unknown errors (a timeout after the request was sent) are the hard part. The reserve
or assign may or may not have happened. We never guess:
  * every step is idempotent, so on an unknown outcome we RETRY the step (with backoff);
  * on a conflict we RE-READ: if the request is already assigned to *this* resource, our
    earlier attempt succeeded and only its answer was lost;
  * we compensate only when the assign has definitely failed (request cancelled or assigned
    elsewhere);
  * if retries run out, the message is redelivered, and step 0 resumes from the reservation
    that is already held (or releases it if the request no longer needs it).

Bug this fixes (found by a chaos run under load): the first version released the reservation
whenever the assign call timed out. When the assign had in fact succeeded, the request ended up
ASSIGNED with no quantity held for it, and a release that timed out left quantity held for nobody.
"""

import asyncio
import logging
from dataclasses import dataclass

from services.common.errors import Conflict, GatewayTimeout, Unavailable
from services.common.geo import haversine_km
from services.common.retry import backoff_delay
from services.matching_service.clients import RequestServiceClient, ResourceServiceClient
from services.matching_service.strategies import MatchingStrategy, RequestView, ResourceView

log = logging.getLogger("matching-saga")

MATCHABLE = {"PENDING", "UNMATCHED"}
HOLDS_ALLOCATION = {"ASSIGNED", "IN_PROGRESS", "FULFILLED"}
CANDIDATE_ROUNDS = 3  # re-query if every candidate was taken by someone else meanwhile
CANDIDATES_PER_ROUND = 5
STEP_RETRIES = 4  # attempts per idempotent step when the outcome is unknown
UNKNOWN_OUTCOME = (Unavailable, GatewayTimeout)


@dataclass
class MatchResult:
    outcome: str  # matched | no_resource | not_matchable
    request_id: str
    request_status: str
    resource_id: str | None = None
    resource_name: str | None = None
    distance_km: float | None = None
    quantity: int | None = None

    def to_json(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None} | {"matched": self.outcome == "matched"}


def _request_view(r: dict) -> RequestView:
    return RequestView(r["id"], r["type"], r["location"]["lat"], r["location"]["lon"], r["quantity"], r["urgency"])


def _resource_view(r: dict) -> ResourceView:
    av = r["availability"]
    return ResourceView(r["id"], r["name"], r["location"]["lat"], r["location"]["lon"],
                        av["quantity_remaining"], av["quantity_total"])


def _holds_for(resource: dict, request_id: str) -> bool:
    return any(a["request_id"] == request_id for a in resource.get("allocations", []))


class MatchingSaga:
    def __init__(self, requests: RequestServiceClient, resources: ResourceServiceClient,
                 strategy: MatchingStrategy, max_radius_km: float, retry_base_s: float = 0.2):
        self.requests = requests
        self.resources = resources
        self.strategy = strategy
        self.max_radius_km = max_radius_km
        self.retry_base_s = retry_base_s

    async def _idempotent(self, call, what: str):
        """Run an idempotent step; on an unknown outcome retry it with backoff (ADR-16).
        If it still fails, the error propagates and the message is redelivered later."""
        for attempt in range(1, STEP_RETRIES + 1):
            try:
                return await call()
            except UNKNOWN_OUTCOME as exc:
                if attempt == STEP_RETRIES:
                    raise
                log.info(f"{what}: outcome unknown ({exc.code}); retrying (attempt {attempt + 1})")
                await asyncio.sleep(backoff_delay(attempt, base=self.retry_base_s))

    async def match(self, request_id: str) -> MatchResult:
        # Step 1: read. NotFound propagates (404 for POST /match; the consumer acks and drops it).
        req = await self._idempotent(lambda: self.requests.get(request_id), "read request")
        view = _request_view(req)
        resources = await self._idempotent(lambda: self.resources.list_by_type(view.type), "list resources")
        held = [r for r in resources if _holds_for(r, request_id)]

        # Step 0: clean up after an interrupted attempt.
        if req["status"] not in MATCHABLE:
            keep = (req.get("assignment") or {}).get("resource_id") if req["status"] in HOLDS_ALLOCATION else None
            for r in held:
                if r["id"] != keep:
                    log.info(f"request {request_id} is {req['status']}; releasing leftover reservation on {r['id']}")
                    await self._idempotent(lambda rid=r["id"]: self.resources.release(rid, request_id), "release leftover")
            if not held:
                log.info(f"request {request_id} is {req['status']}; nothing to do (idempotent skip)")
            return MatchResult("not_matchable", request_id, req["status"])

        if held:
            # Step 0: resume. Quantity is already reserved for this request: assign it there.
            # Other reservations for the same request are NOT released here: they may belong to a
            # duplicate saga that is still running (a retry, a wake-up or the reconciler's resend).
            # That saga releases its own if its assign loses; one that died is cleaned up by the
            # not-matchable branch above on a later delivery. (Releasing them here caused a
            # request to be assigned to a resource whose reservation had just been given back.)
            res = _resource_view(held[0])
            distance = round(haversine_km(view.lat, view.lon, res.lat, res.lon), 2)
            log.info(f"request {request_id}: resuming with the reservation already held on {res.id}")
            return await self._assign(req, res, distance)

        # Steps 2 + 3: rank candidates, reserve the best one that still has enough quantity.
        for round_ in range(CANDIDATE_ROUNDS):
            if round_ > 0:
                resources = await self._idempotent(lambda: self.resources.list_by_type(view.type), "list resources")
            available = [_resource_view(r) for r in resources if r["status"] == "AVAILABLE"]
            ranked = self.strategy.rank(view, available, self.max_radius_km)
            if not ranked:
                break
            for cand in ranked[:CANDIDATES_PER_ROUND]:
                try:
                    await self._idempotent(
                        lambda rid=cand.resource.id: self.resources.reserve(rid, request_id, view.quantity), "reserve")
                except Conflict as exc:  # INSUFFICIENT_QUANTITY / NOT_AVAILABLE: someone else got it
                    log.info(f"candidate {cand.resource.id} lost ({exc.code}); trying next")
                    continue
                return await self._assign(req, cand.resource, cand.distance_km)  # step 4

        # No resource: record it so the person sees "waiting for supply" (retries happen in the
        # background). Only the PENDING -> UNMATCHED change is written; re-writing an already
        # UNMATCHED request on every retry only added load (seen in a chaos run).
        status = req["status"]
        if status == "PENDING":
            try:
                status = (await self._idempotent(lambda: self.requests.mark_unmatched(request_id), "mark unmatched"))["status"]
            except Conflict:  # meanwhile cancelled/assigned by someone else: fine
                status = (await self.requests.get(request_id))["status"]
        log.info(f"no suitable {view.type} resource within {self.max_radius_km} km for {request_id}")
        return MatchResult("no_resource", request_id, status)

    async def _assign(self, req: dict, res: ResourceView, distance_km: float) -> MatchResult:
        """Step 4, with compensation only on a definite failure."""
        request_id, version = req["id"], req["version"]
        for _ in range(5):
            try:
                assigned = await self._idempotent(
                    lambda v=version: self.requests.assign(request_id, res.id, req["quantity"], distance_km, if_match=v),
                    "assign")
                log.info("matched", extra={"fields": {"request_id": request_id, "resource_id": res.id,
                                                      "distance_km": distance_km, "strategy": self.strategy.name}})
                return MatchResult("matched", request_id, assigned["status"], res.id, res.name,
                                   distance_km, req["quantity"])
            except Conflict:  # VERSION_CONFLICT, NOT_MATCHABLE or INVALID_TRANSITION: look at the truth
                latest = await self._idempotent(lambda: self.requests.get(request_id), "read request")
                ours = (latest.get("assignment") or {}).get("resource_id") == res.id
                if latest["status"] in HOLDS_ALLOCATION and ours:
                    # An earlier assign of ours succeeded; only its answer was lost (timeout).
                    log.info(f"request {request_id} is already assigned to {res.id}: earlier attempt succeeded")
                    return MatchResult("matched", request_id, latest["status"], res.id, res.name,
                                       distance_km, req["quantity"])
                if latest["status"] in MATCHABLE:
                    version = latest["version"]  # it only changed in a harmless way: try again
                    continue
                break  # definitely not ours: cancelled, or assigned to another resource

        # COMPENSATION: the request can't take this quantity, so give it back (idempotent).
        await self._idempotent(lambda: self.resources.release(res.id, request_id), "release (compensation)")
        latest = await self.requests.get(request_id)
        log.info(f"assign failed for {request_id} (now {latest['status']}); reservation on {res.id} released")
        return MatchResult("not_matchable", request_id, latest["status"])

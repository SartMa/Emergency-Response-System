"""The matching saga (DESIGN.md §6.2, ADR-09). Used by the RabbitMQ consumer AND by
POST /match/{id}, so asynchronous and on-demand matching behave identically.

A request and a resource live on different shards and are owned by different services, so
"reserve the resource AND assign the request" cannot be one database transaction. Instead:

  1. read the request              (skip if it is no longer PENDING/UNMATCHED: idempotent consumer)
  2. find + rank candidates        (scatter-gather over the shards, then MatchingStrategy)
  3. RESERVE quantity on the best  (compare-and-set inside resource-service, cannot oversell)
  4. ASSIGN the request            (If-Match on the request version)
     if 4 fails -> COMPENSATE:     release the reservation from step 3

Every step is idempotent, so a redelivered message or two concurrent sagas for the same
request are harmless: at most one ASSIGN succeeds, and the loser releases what it reserved.
"""

import logging
from dataclasses import dataclass

from services.common.errors import Conflict, Unavailable, VersionConflict
from services.matching_service.clients import RequestServiceClient, ResourceServiceClient
from services.matching_service.strategies import MatchingStrategy, RequestView, ResourceView

log = logging.getLogger("matching-saga")

MATCHABLE = {"PENDING", "UNMATCHED"}
CANDIDATE_ROUNDS = 3  # re-query if every candidate was taken by someone else meanwhile
CANDIDATES_PER_ROUND = 5


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


class MatchingSaga:
    def __init__(self, requests: RequestServiceClient, resources: ResourceServiceClient,
                 strategy: MatchingStrategy, max_radius_km: float):
        self.requests = requests
        self.resources = resources
        self.strategy = strategy
        self.max_radius_km = max_radius_km

    async def match(self, request_id: str) -> MatchResult:
        # Step 1: read. NotFound propagates (404 for POST /match; the consumer acks and drops it).
        req = await self.requests.get(request_id)
        if req["status"] not in MATCHABLE:
            log.info(f"request {request_id} is {req['status']}; nothing to do (idempotent skip)")
            return MatchResult("not_matchable", request_id, req["status"])
        view = _request_view(req)

        # Steps 2 + 3: rank candidates, reserve the best one that still has enough quantity.
        for _ in range(CANDIDATE_ROUNDS):
            raw = await self.resources.list_available(view.type)
            ranked = self.strategy.rank(view, [_resource_view(r) for r in raw], self.max_radius_km)
            if not ranked:
                break
            for cand in ranked[:CANDIDATES_PER_ROUND]:
                try:
                    await self.resources.reserve(cand.resource.id, request_id, view.quantity)
                except Conflict as exc:  # INSUFFICIENT_QUANTITY / NOT_AVAILABLE: someone else got it
                    log.info(f"candidate {cand.resource.id} lost ({exc.code}); trying next")
                    continue
                # Step 4: assign (with compensation on failure)
                return await self._assign_or_compensate(req, cand.resource, cand.distance_km)

        # No resource: record it so the person can see we tried (Tier 2 retries automatically).
        try:
            updated = await self.requests.mark_unmatched(request_id)
            status = updated["status"]
        except Conflict:  # meanwhile cancelled/assigned by someone else: fine
            status = (await self.requests.get(request_id))["status"]
        log.info(f"no suitable {view.type} resource within {self.max_radius_km} km for {request_id}")
        return MatchResult("no_resource", request_id, status)

    async def _assign_or_compensate(self, req: dict, res: ResourceView, distance_km: float) -> MatchResult:
        try:
            return await self._assign(req, res, distance_km)
        except Unavailable:
            # request-service/storage is briefly down after we reserved. Undo the reservation
            # (best effort) before the message is requeued, so a retry that picks a different
            # resource does not leave this one holding quantity for nobody.
            try:
                await self.resources.release(res.id, req["id"])
            except Exception:
                log.error(f"could not release {res.id} for {req['id']}; quantity may be held (known limitation)")
            raise

    async def _assign(self, req: dict, res: ResourceView, distance_km: float) -> MatchResult:
        request_id, version = req["id"], req["version"]
        for _ in range(3):
            try:
                assigned = await self.requests.assign(request_id, res.id, req["quantity"], distance_km, if_match=version)
                log.info("matched", extra={"fields": {"request_id": request_id, "resource_id": res.id,
                                                      "distance_km": distance_km, "strategy": self.strategy.name}})
                return MatchResult("matched", request_id, assigned["status"], res.id, res.name,
                                   distance_km, req["quantity"])
            except VersionConflict:
                # The request changed since we read it. If it is still matchable (e.g. only its
                # attempt counter moved) retry with the new version; otherwise compensate.
                latest = await self.requests.get(request_id)
                if latest["status"] not in MATCHABLE:
                    break
                version = latest["version"]
            except Conflict:  # NOT_MATCHABLE / INVALID_TRANSITION
                break

        # COMPENSATION: give the quantity back, because the request can no longer take it.
        await self.resources.release(res.id, request_id)
        latest = await self.requests.get(request_id)
        log.info(f"assign failed for {request_id} (now {latest['status']}); reservation on {res.id} released")
        return MatchResult("not_matchable", request_id, latest["status"])

"""Request application service: orchestrates domain + ports (storage, message broker).

Every read-modify-write is optimistic (ADR-06): read (value, version) -> change in memory ->
write with expected_version. If someone else wrote in between, storage answers 409 and we
either re-read and retry (when the caller did not pin a version) or report the conflict
(when the caller sent If-Match).
"""

import logging
from datetime import datetime, timezone

from services.common.amqp import COMMANDS_EXCHANGE, EVENTS_EXCHANGE, MATCH_ROUTING_KEY, AmqpClient
from services.common.config import env_float
from services.common.errors import Unavailable, VersionConflict
from services.common.models import request_key
from services.common.storage_client import StorageClient
from services.request_service import domain
from services.request_service.domain import Assignment, CreateRequest, EmergencyRequest, RequestStatus

log = logging.getLogger("request-service")

MAX_CAS_RETRIES = 5
RECONCILE_MIN_AGE_S = env_float("RECONCILE_MIN_AGE_S", 30.0)
RECONCILE_RESEND_EVERY_S = env_float("RECONCILE_RESEND_EVERY_S", 60.0)


class RequestService:
    def __init__(self, storage: StorageClient, broker: AmqpClient):
        self.storage = storage
        self.broker = broker
        # request id -> when this pod last re-sent it (per pod; another pod may also re-send,
        # which only produces a harmless duplicate)
        self._resent: dict[str, datetime] = {}

    async def reconcile_pending(self) -> int:
        """Reconciliation loop body (DESIGN.md §6.2, same idea as a Kubernetes controller):
        compare desired state (every PENDING request has a match command in flight) with actual
        state, and repair the difference by re-sending. Returns how many were re-sent."""
        if not self.broker.is_ready:
            return 0  # nothing we could do this round anyway
        items, _ = await self.storage.query("req", None, RequestStatus.PENDING)
        reqs = [EmergencyRequest.model_validate(i.value) for i in items]
        now = datetime.now(timezone.utc)
        stale = domain.stale_pending(reqs, now, RECONCILE_MIN_AGE_S, self._resent, RECONCILE_RESEND_EVERY_S)
        resent = 0
        for req in stale:
            if await self.publish_match(req):
                self._resent[req.id] = now
                resent += 1
        # forget entries that can no longer matter, so the dict does not grow forever
        self._resent = {k: t for k, t in self._resent.items()
                        if (now - t).total_seconds() < 2 * RECONCILE_RESEND_EVERY_S}
        if resent:
            log.warning(f"reconciler: re-sent {resent} request(s) stuck in PENDING for over {RECONCILE_MIN_AGE_S:.0f}s")
        return resent

    async def create(self, body: CreateRequest) -> tuple[EmergencyRequest, int, bool]:
        """Spec §7 steps 1-3: store the request, then publish it for asynchronous matching.
        Returns (request, version, queued). If RabbitMQ is down the request is still stored and
        the client still gets 201 with queued=false (NFR-11)."""
        req = domain.new_request(body)
        version = await self.storage.create(request_key(req.id), req.model_dump(mode="json"))
        queued = await self.publish_match(req)
        return req, version, queued

    async def publish_match(self, req: EmergencyRequest) -> bool:
        try:
            # Message priority = urgency, so urgent requests overtake the queue (FR-16).
            await self.broker.publish(
                COMMANDS_EXCHANGE, MATCH_ROUTING_KEY,
                {"request_id": req.id, "type": req.type, "urgency": req.urgency},
                priority=req.urgency,
            )
            log.info("published match command", extra={"fields": {"request_id": req.id, "priority": req.urgency}})
            return True
        except Unavailable as exc:
            log.warning(f"could not queue request {req.id} for matching: {exc.message}")
            return False

    async def get(self, request_id: str) -> tuple[EmergencyRequest, int]:
        item = await self.storage.get(request_key(request_id))
        return EmergencyRequest.model_validate(item.value), item.version

    async def list(self, status: str | None, type_: str | None) -> tuple[list[EmergencyRequest], bool]:
        items, partial = await self.storage.query("req", type_, status)
        reqs = [EmergencyRequest.model_validate(i.value) for i in items]
        reqs.sort(key=lambda r: r.created_at, reverse=True)
        return reqs, partial

    async def _update(self, request_id: str, change, if_match: int | None = None) -> tuple[EmergencyRequest, int, EmergencyRequest]:
        """Optimistic read-modify-write. `change(req) -> new_req` is pure domain logic.
        Returns (new, new_version, old)."""
        for _ in range(MAX_CAS_RETRIES):
            old, version = await self.get(request_id)
            if if_match is not None and if_match != version:
                raise VersionConflict(f"Request is at version {version}, not {if_match}",
                                      details={"current_version": version})
            new = change(old)
            try:
                new_version = await self.storage.put(request_key(request_id), new.model_dump(mode="json"),
                                                     expected_version=version)
                return new, new_version, old
            except VersionConflict:
                if if_match is not None:
                    raise  # the caller pinned a version; let them decide what to do
                continue  # somebody else wrote first: re-read and try again
        raise VersionConflict("Too much contention on this request, try again")

    async def update_status(self, request_id: str, status: RequestStatus, note: str | None,
                            if_match: int | None) -> tuple[EmergencyRequest, int]:
        if status not in domain.CLIENT_SETTABLE:
            raise domain.InvalidTransition(
                f"Clients may only set {sorted(domain.CLIENT_SETTABLE)}; {status} is set by the system")
        new, version, old = await self._update(request_id, lambda r: domain.transition(r, status, note), if_match)

        if status == RequestStatus.CANCELLED and old.assignment is not None:
            # Saga compensation (FR-20): the resource must get its quantity back. resource-service
            # listens for this event (choreography), so this service never writes resources itself.
            await self._publish_event("request.cancelled", new)
        else:
            await self._publish_event(f"request.{status.lower()}", new)
        return new, version

    async def assign(self, request_id: str, assignment: Assignment, if_match: int | None) -> tuple[EmergencyRequest, int]:
        """Internal, called by matching-service (saga step 'assign')."""
        new, version, _ = await self._update(request_id, lambda r: domain.assign(r, assignment), if_match)
        await self._publish_event("request.assigned", new)
        return new, version

    async def mark_unmatched(self, request_id: str) -> tuple[EmergencyRequest, int]:
        new, version, _ = await self._update(request_id, domain.mark_unmatched)
        return new, version

    async def _publish_event(self, routing_key: str, req: EmergencyRequest) -> None:
        body = {"request_id": req.id, "status": req.status, "type": req.type}
        if req.assignment:
            body["resource_id"] = req.assignment.resource_id
            body["quantity"] = req.assignment.quantity
        try:
            await self.broker.publish(EVENTS_EXCHANGE, routing_key, body)
        except Unavailable as exc:
            # Tier 2: the reconciler would retry this. Logged loudly so it is visible meanwhile.
            log.error(f"could not publish {routing_key} for {req.id}: {exc.message}")

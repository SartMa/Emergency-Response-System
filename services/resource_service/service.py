"""Resource application service.

Reserve is where over-allocation is prevented (FR-18). Two matching pods may try to take
the last units of the same tanker at the same moment. Both read version 7; both try to write
with expected_version=7; storage accepts exactly one and answers 409 to the other, which
re-reads (now version 8, fewer units) and re-checks. No locks, and no way to oversell
(ADR-06, DDIA ch. 8 "Preventing lost updates", compare-and-set).
"""

import asyncio
import logging
import random

from services.common.amqp import EVENTS_EXCHANGE, AmqpClient, PermanentError
from services.common.errors import NotFound, Unavailable, VersionConflict
from services.common.models import resource_key
from services.common.storage_client import StorageClient
from services.resource_service import domain
from services.resource_service.domain import RegisterResource, Resource, ResourceStatus

log = logging.getLogger("resource-service")

MAX_CAS_RETRIES = 20  # contention on one popular resource can be high during a burst


class ResourceService:
    def __init__(self, storage: StorageClient, broker: AmqpClient):
        self.storage = storage
        self.broker = broker

    async def register(self, body: RegisterResource) -> tuple[Resource, int]:
        res = domain.new_resource(body)
        version = await self.storage.create(resource_key(res.id), res.model_dump(mode="json"))
        await self._announce_supply("resource.registered", res)
        return res, version

    async def _announce_supply(self, event: str, res: Resource) -> None:
        """Tell matching that supply appeared or grew, so UNMATCHED requests of this type near
        this location get another attempt (wake-up, FR-19). Best effort: if RabbitMQ is down,
        the waiting requests are not lost, they just wait for the next supply event."""
        try:
            await self.broker.publish(EVENTS_EXCHANGE, event, {
                "event": event, "resource_id": res.id, "type": res.type,
                "lat": res.location.lat, "lon": res.location.lon,
                "quantity_remaining": res.availability.quantity_remaining,
            })
        except Unavailable:
            log.warning(f"could not publish {event} for {res.id}")

    async def get(self, resource_id: str) -> tuple[Resource, int]:
        item = await self.storage.get(resource_key(resource_id))
        return Resource.model_validate(item.value), item.version

    async def list(self, type_: str | None, status: str | None) -> tuple[list[tuple[Resource, int]], bool]:
        items, partial = await self.storage.query("res", type_, status)
        out = [(Resource.model_validate(i.value), i.version) for i in items]
        out.sort(key=lambda rv: rv[0].created_at)
        return out, partial

    async def _update(self, resource_id: str, change, if_match: int | None = None) -> tuple[Resource, int, bool]:
        """Optimistic read-modify-write; `change(res) -> (new_res, changed)` is pure domain logic."""
        for attempt in range(MAX_CAS_RETRIES):
            old, version = await self.get(resource_id)
            if if_match is not None and if_match != version:
                raise VersionConflict(f"Resource is at version {version}, not {if_match}",
                                      details={"current_version": version})
            new, changed = change(old)
            if not changed:
                return old, version, False
            try:
                new_version = await self.storage.put(resource_key(resource_id), new.model_dump(mode="json"),
                                                     expected_version=version)
                return new, new_version, True
            except VersionConflict:
                if if_match is not None:
                    raise
                # Lost the race. Back off a little (with jitter, so the losers don't collide again).
                await asyncio.sleep(random.uniform(0, 0.01 * (attempt + 1)))
        raise VersionConflict("Too much contention on this resource, try again")

    async def reserve(self, resource_id: str, request_id: str, quantity: int,
                      if_match: int | None = None) -> tuple[Resource, int]:
        res, version, changed = await self._update(
            resource_id, lambda r: domain.reserve(r, request_id, quantity), if_match)
        if changed:
            log.info("reserved", extra={"fields": {"resource_id": resource_id, "request_id": request_id,
                                                   "quantity": quantity,
                                                   "remaining": res.availability.quantity_remaining}})
        return res, version

    async def release(self, resource_id: str, request_id: str) -> tuple[Resource, int]:
        res, version, changed = await self._update(resource_id, lambda r: domain.release(r, request_id))
        if changed:
            log.info("released (compensation)", extra={"fields": {
                "resource_id": resource_id, "request_id": request_id,
                "remaining": res.availability.quantity_remaining}})
            if res.status == ResourceStatus.AVAILABLE:
                await self._announce_supply("resource.released", res)
        return res, version

    async def update_status(self, resource_id: str, status: ResourceStatus | None,
                            quantity_remaining: int | None, if_match: int | None) -> tuple[Resource, int]:
        before, _ = await self.get(resource_id)
        res, version, _ = await self._update(
            resource_id, lambda r: (domain.update_availability(r, status, quantity_remaining), True), if_match)
        if domain.gained_supply(before, res):
            await self._announce_supply("resource.restocked", res)
        return res, version

    async def on_request_cancelled(self, event: dict) -> None:
        """Consumer of `request.cancelled` (choreography): undo the reservation (FR-20)."""
        resource_id, request_id = event.get("resource_id"), event.get("request_id")
        if not request_id:
            raise PermanentError(f"malformed request.cancelled event: {event}")
        if not resource_id:
            # The request was cancelled before it was ever assigned: nothing is reserved, so there
            # is nothing to give back. (These used to be dead-lettered by mistake.)
            return
        try:
            await self.release(resource_id, request_id)
        except NotFound:
            log.warning(f"release: resource {resource_id} no longer exists; nothing to give back")

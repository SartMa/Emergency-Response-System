"""Resource domain: volunteers, NGOs and supplies, with a quantity (FR-17, DESIGN.md §4.4).

Quantity is ONE integer on ONE record. The units are interchangeable (one food packet is as
good as another), so they need no IDs of their own. Things that are not interchangeable,
like a particular volunteer or ambulance, are registered as separate resources with quantity 1.

`allocations` records who got how much, so a cancellation gives back exactly that amount,
and so that reserving twice for the same request is harmless (idempotent).
"""

import uuid
from enum import StrEnum

from pydantic import BaseModel, Field

from services.common.errors import BadRequest, Conflict
from services.common.models import Location, ResourceType, utc_now


class ResourceStatus(StrEnum):
    AVAILABLE = "AVAILABLE"  # online and quantity_remaining > 0
    EXHAUSTED = "EXHAUSTED"  # online but nothing left
    OFFLINE = "OFFLINE"  # switched off by the provider


class Provider(StrEnum):
    VOLUNTEER = "VOLUNTEER"
    NGO = "NGO"
    SUPPLY = "SUPPLY"


class Availability(BaseModel):
    quantity_total: int = Field(ge=0)
    quantity_remaining: int = Field(ge=0)
    online: bool = True


class Allocation(BaseModel):
    request_id: str
    quantity: int
    at: str


class RegisterResource(BaseModel):
    """Body of POST /resources: registerResource(type, location, availability)."""

    type: ResourceType
    location: Location
    name: str = Field(min_length=1, max_length=120)
    provider: Provider = Provider.VOLUNTEER
    quantity: int = Field(default=1, ge=1, le=1_000_000, description="availability: units this resource can provide")


class Resource(BaseModel):
    id: str
    type: ResourceType
    name: str
    provider: Provider
    location: Location
    availability: Availability
    status: ResourceStatus
    allocations: list[Allocation] = Field(default_factory=list)
    created_at: str
    updated_at: str


class InsufficientQuantity(Conflict):
    code = "INSUFFICIENT_QUANTITY"


class NotAvailable(Conflict):
    code = "RESOURCE_NOT_AVAILABLE"


def derive_status(av: Availability) -> ResourceStatus:
    """Status is derived, never set independently, so it can't contradict the numbers."""
    if not av.online:
        return ResourceStatus.OFFLINE
    return ResourceStatus.AVAILABLE if av.quantity_remaining > 0 else ResourceStatus.EXHAUSTED


def new_resource(body: RegisterResource) -> Resource:
    now = utc_now()
    av = Availability(quantity_total=body.quantity, quantity_remaining=body.quantity)
    return Resource(
        id=str(uuid.uuid4()), type=body.type, name=body.name, provider=body.provider,
        location=body.location, availability=av, status=derive_status(av),
        created_at=now, updated_at=now,
    )


def _with_availability(res: Resource, av: Availability, allocations: list[Allocation] | None = None) -> Resource:
    return res.model_copy(update={
        "availability": av,
        "status": derive_status(av),
        "allocations": res.allocations if allocations is None else allocations,
        "updated_at": utc_now(),
    })


def reserve(res: Resource, request_id: str, quantity: int) -> tuple[Resource, bool]:
    """Take `quantity` units for `request_id`. Returns (resource, changed).
    Idempotent: if this request already holds an allocation, nothing changes. A redelivered
    message or a retried saga step must not take the quantity twice."""
    if any(a.request_id == request_id for a in res.allocations):
        return res, False
    if res.status == ResourceStatus.OFFLINE:
        raise NotAvailable(f"Resource {res.id} is offline")
    if res.availability.quantity_remaining < quantity:
        raise InsufficientQuantity(
            f"Resource {res.id} has {res.availability.quantity_remaining} left, {quantity} requested",
            details={"remaining": res.availability.quantity_remaining, "requested": quantity},
        )
    av = res.availability.model_copy(update={"quantity_remaining": res.availability.quantity_remaining - quantity})
    allocation = Allocation(request_id=request_id, quantity=quantity, at=utc_now())
    return _with_availability(res, av, [*res.allocations, allocation]), True


def release(res: Resource, request_id: str) -> tuple[Resource, bool]:
    """Saga compensation: give back what `request_id` took. Idempotent (no allocation -> no-op)."""
    allocation = next((a for a in res.allocations if a.request_id == request_id), None)
    if allocation is None:
        return res, False
    av = res.availability.model_copy(update={
        "quantity_remaining": min(res.availability.quantity_total,
                                  res.availability.quantity_remaining + allocation.quantity)
    })
    return _with_availability(res, av, [a for a in res.allocations if a.request_id != request_id]), True


def update_availability(res: Resource, status: ResourceStatus | None, quantity_remaining: int | None) -> Resource:
    """PUT /resources/{id}/status: switch online/offline and/or restock."""
    if status == ResourceStatus.EXHAUSTED:
        raise BadRequest("EXHAUSTED is derived from quantity; set quantity_remaining to 0 instead")
    av = res.availability.model_copy()
    if status is not None:
        av.online = status == ResourceStatus.AVAILABLE
    if quantity_remaining is not None:
        av.quantity_remaining = quantity_remaining
        av.quantity_total = max(av.quantity_total, quantity_remaining)
    return _with_availability(res, av)

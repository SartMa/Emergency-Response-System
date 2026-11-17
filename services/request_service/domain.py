r"""Request domain: the emergency request entity and its lifecycle (DESIGN.md §4.4).

Pure logic (no I/O), so it is unit-tested directly. The lifecycle is a table-driven state
machine (State pattern): every allowed move is listed in TRANSITIONS, everything else is
rejected with 409 INVALID_TRANSITION.

    PENDING --match--> ASSIGNED --> IN_PROGRESS --> FULFILLED
       |  \--no resource--> UNMATCHED --match--> ASSIGNED
       \------------------+---------------+-----------------> CANCELLED
"""

import uuid
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field

from services.common.errors import Conflict
from services.common.models import Location, ResourceType, utc_now


class RequestStatus(StrEnum):
    PENDING = "PENDING"
    UNMATCHED = "UNMATCHED"
    ASSIGNED = "ASSIGNED"
    IN_PROGRESS = "IN_PROGRESS"
    FULFILLED = "FULFILLED"
    CANCELLED = "CANCELLED"


S = RequestStatus
TRANSITIONS: dict[RequestStatus, set[RequestStatus]] = {
    S.PENDING: {S.ASSIGNED, S.UNMATCHED, S.CANCELLED},
    S.UNMATCHED: {S.ASSIGNED, S.CANCELLED},
    S.ASSIGNED: {S.IN_PROGRESS, S.FULFILLED, S.CANCELLED},
    S.IN_PROGRESS: {S.FULFILLED, S.CANCELLED},
    S.FULFILLED: set(),  # terminal
    S.CANCELLED: set(),  # terminal
}
# Clients may only move a request forward or cancel it. ASSIGNED/UNMATCHED are set by matching.
CLIENT_SETTABLE = {S.IN_PROGRESS, S.FULFILLED, S.CANCELLED}
MATCHABLE = {S.PENDING, S.UNMATCHED}


class InvalidTransition(Conflict):
    code = "INVALID_TRANSITION"


class CreateRequest(BaseModel):
    """Body of POST /requests: createRequest(type, location, details) + our optional extras."""

    type: ResourceType
    location: Location
    details: str = Field(default="", max_length=1000)
    urgency: int = Field(default=3, ge=1, le=5, description="1 = low ... 5 = life-threatening (FR-16)")
    quantity: int = Field(default=1, ge=1, le=10_000, description="units needed (FR-17)")


class Assignment(BaseModel):
    resource_id: str
    quantity: int
    distance_km: float


class EmergencyRequest(BaseModel):
    id: str
    type: ResourceType
    location: Location
    details: str
    urgency: int
    quantity: int
    status: RequestStatus
    assignment: Assignment | None = None
    match_attempts: int = 0
    history: list[dict] = Field(default_factory=list)  # audit trail (FR-23)
    created_at: str
    updated_at: str


def new_request(body: CreateRequest) -> EmergencyRequest:
    now = utc_now()
    return EmergencyRequest(
        id=str(uuid.uuid4()),
        type=body.type,
        location=body.location,
        details=body.details,
        urgency=body.urgency,
        quantity=body.quantity,
        status=S.PENDING,
        history=[{"status": S.PENDING, "at": now}],
        created_at=now,
        updated_at=now,
    )


def transition(req: EmergencyRequest, new_status: RequestStatus, note: str | None = None) -> EmergencyRequest:
    """Return a copy of `req` in `new_status`, or raise InvalidTransition."""
    if new_status not in TRANSITIONS[req.status]:
        raise InvalidTransition(
            f"Cannot change status from {req.status} to {new_status}",
            details={"from": req.status, "to": new_status, "allowed": sorted(TRANSITIONS[req.status])},
        )
    now = utc_now()
    entry = {"status": new_status, "at": now}
    if note:
        entry["note"] = note
    return req.model_copy(update={"status": new_status, "updated_at": now, "history": [*req.history, entry]})


def assign(req: EmergencyRequest, assignment: Assignment) -> EmergencyRequest:
    if req.status not in MATCHABLE:
        raise Conflict(f"Request is {req.status}, it cannot be matched", code="NOT_MATCHABLE",
                       details={"status": req.status})
    updated = transition(req, S.ASSIGNED, note=f"resource {assignment.resource_id} at {assignment.distance_km} km")
    return updated.model_copy(update={"assignment": assignment, "match_attempts": req.match_attempts + 1})


def stale_pending(reqs: list, now: datetime, min_age_s: float,
                  recently_resent: dict[str, datetime], resend_every_s: float) -> list:
    """Reconciler rule (DESIGN.md §6.2): which requests look like their match command was lost?

    A request should leave PENDING within a second or two (matching marks it ASSIGNED or
    UNMATCHED). One still PENDING after `min_age_s` was probably stored while RabbitMQ was down,
    or its publish failed. We re-send it, but not more often than every `resend_every_s`, so a
    long queue backlog does not get flooded with duplicates. Duplicates are harmless anyway:
    the matching consumer skips requests that are no longer PENDING/UNMATCHED."""
    out = []
    for r in reqs:
        if r.status != S.PENDING:
            continue
        age = (now - datetime.fromisoformat(r.created_at)).total_seconds()
        last = recently_resent.get(r.id)
        if age >= min_age_s and (last is None or (now - last).total_seconds() >= resend_every_s):
            out.append(r)
    return out


def mark_unmatched(req: EmergencyRequest) -> EmergencyRequest:
    """No suitable resource right now. PENDING -> UNMATCHED; already UNMATCHED just counts the attempt."""
    if req.status == S.UNMATCHED:
        return req.model_copy(update={"match_attempts": req.match_attempts + 1, "updated_at": utc_now()})
    updated = transition(req, S.UNMATCHED, note="no suitable resource available")
    return updated.model_copy(update={"match_attempts": req.match_attempts + 1})

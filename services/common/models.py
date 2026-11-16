"""Shared value types. Entity models (Request, Resource) live in the owning service's domain."""

from datetime import datetime, timezone
from enum import StrEnum

from pydantic import BaseModel, Field


class ResourceType(StrEnum):
    WATER = "water"
    FOOD = "food"
    MEDICINE = "medicine"
    TRANSPORTATION = "transportation"
    SHELTER = "shelter"


class Location(BaseModel):
    """Sent by the client (phone GPS / operator). See ADR-15."""

    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    label: str | None = Field(default=None, max_length=120)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# Storage key namespaces: database-per-service, done logically (DESIGN.md §4.3).
REQUEST_PREFIX = "req"
RESOURCE_PREFIX = "res"


def request_key(request_id: str) -> str:
    return f"{REQUEST_PREFIX}:{request_id}"


def resource_key(resource_id: str) -> str:
    return f"{RESOURCE_PREFIX}:{resource_id}"

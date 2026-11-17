"""Pure decision rules for retries and wake-ups (FR-19). No I/O, so they are unit-tested directly.

Lifecycle of a request that finds no resource:

  attempt 0  (first try, from request-service)  -> no resource -> UNMATCHED, retry in 2 s
  attempt 1..4                                   -> no resource -> retry in 4, 8, 16, 32 s
  attempt 5                                      -> no resource -> stop retrying; stays UNMATCHED
  any time: a supply event (new / restocked / freed resource of the same type, nearby)
            -> one more attempt ("wake-up"), without restarting the whole retry chain

The request becomes UNMATCHED on the FIRST failure, not after the last retry. That way the
person immediately sees "waiting for supply", and the reconciler (which re-sends requests stuck
in PENDING) never mistakes a request that is merely waiting for a lost one.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from services.common.geo import haversine_km


def next_retry_delay(attempt: int, delays: Sequence[int]) -> int | None:
    """Delay (seconds) before the next attempt, or None when the retry budget is used up.
    `attempt` counts the attempts already made, starting at 0 for the first one."""
    return delays[attempt] if 0 <= attempt < len(delays) else None


@dataclass(frozen=True)
class WaitingRequest:
    id: str
    type: str
    lat: float
    lon: float
    urgency: int
    created_at: str
    quantity: int = 1


@dataclass(frozen=True)
class Supply:
    """What one supply event offers: units available now, at a place (None = unknown place)."""

    lat: float | None
    lon: float | None
    units: int


def select_for_wakeup(waiting: list[WaitingRequest], type_: str, supplies: list[Supply],
                      max_radius_km: float, limit: int) -> list[WaitingRequest]:
    """Which UNMATCHED requests should get another attempt after supply events of this type?

    Wake only as many requests as the new supply could actually serve. Walk the waiting
    requests, most urgent first and then longest waiting; give each one to a supply within the
    matching radius that still has enough units in our simulation, and wake it only if one
    exists. A release of 2 units therefore wakes at most 2 single-unit requests.

    Bug this prevents (seen in a chaos run): the first version woke the same 50 requests on
    every event, whatever the event offered. With frequent small releases, each of those
    requests was re-matched and re-written ~37 times, overloading the whole system."""
    budget = [s.units for s in supplies]
    chosen: list[WaitingRequest] = []
    for r in sorted((r for r in waiting if r.type == type_), key=lambda r: (-r.urgency, r.created_at)):
        if len(chosen) >= limit:
            break
        for i, s in enumerate(supplies):
            near = s.lat is None or s.lon is None or haversine_km(s.lat, s.lon, r.lat, r.lon) <= max_radius_km
            if near and budget[i] >= r.quantity:
                budget[i] -= r.quantity
                chosen.append(r)
                break
    return chosen

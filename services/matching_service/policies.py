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


def select_for_wakeup(waiting: list[WaitingRequest], type_: str, lat: float | None, lon: float | None,
                      max_radius_km: float, limit: int) -> list[WaitingRequest]:
    """Which UNMATCHED requests should get another attempt after a supply event?

    Only the same type, only within the matching radius of the new supply (a tanker in Thane
    can't help someone in Pune), most urgent first, then longest waiting. `limit` stops one
    event from flooding the queue."""
    candidates = [r for r in waiting if r.type == type_]
    if lat is not None and lon is not None:
        candidates = [r for r in candidates if haversine_km(lat, lon, r.lat, r.lon) <= max_radius_km]
    candidates.sort(key=lambda r: (-r.urgency, r.created_at))
    return candidates[:limit]

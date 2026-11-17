"""Unit tests for the Tier 2 rules: retry schedule, wake-up selection, supply detection, reconciler."""

from datetime import datetime, timedelta, timezone

from services.common.amqp import RETRY_DELAYS_S
from services.common.models import Location
from services.matching_service.policies import Supply, WaitingRequest, next_retry_delay, select_for_wakeup
from services.resource_service import domain as rs


# ---------------------------------------------------------------- retry schedule
def test_retry_schedule_is_exponential_then_stops():
    delays = [next_retry_delay(a, RETRY_DELAYS_S) for a in range(7)]
    assert delays == [2, 4, 8, 16, 32, None, None]
    assert all(b == 2 * a for a, b in zip(RETRY_DELAYS_S, RETRY_DELAYS_S[1:]))  # each wait doubles


# ---------------------------------------------------------------- wake-up selection
def waiting(id_, type_="water", lat=19.0, lon=72.8, urgency=3, created="2026-11-17T10:00:00Z", quantity=1):
    return WaitingRequest(id_, type_, lat, lon, urgency, created, quantity)


def test_wakeup_only_same_type_and_nearby():
    reqs = [waiting("near"), waiting("other-type", type_="food"), waiting("far", lat=20.0)]  # 20.0 is ~111 km away
    chosen = select_for_wakeup(reqs, "water", [Supply(19.01, 72.8, units=10)], max_radius_km=50, limit=10)
    assert [r.id for r in chosen] == ["near"]


def test_wakeup_most_urgent_then_longest_waiting_and_limited():
    reqs = [waiting("old-low", urgency=2, created="2026-11-17T09:00:00Z"),
            waiting("new-high", urgency=5, created="2026-11-17T11:00:00Z"),
            waiting("old-high", urgency=5, created="2026-11-17T08:00:00Z")]
    chosen = select_for_wakeup(reqs, "water", [Supply(None, None, units=10)], max_radius_km=50, limit=2)
    assert [r.id for r in chosen] == ["old-high", "new-high"]


def test_wakeup_never_wakes_more_than_the_supply_can_serve():
    """The chaos-run bug: 50 requests woken for a release of 2 units, every time."""
    reqs = [waiting(f"r{i}", created=f"2026-11-17T10:00:{i:02d}Z") for i in range(50)]
    chosen = select_for_wakeup(reqs, "water", [Supply(19.0, 72.8, units=2)], max_radius_km=50, limit=50)
    assert [r.id for r in chosen] == ["r0", "r1"]


def test_wakeup_skips_requests_too_big_for_the_supply_and_combines_events():
    reqs = [waiting("needs-10", quantity=10, urgency=5), waiting("needs-3", quantity=3), waiting("needs-1", quantity=1)]
    one_event = select_for_wakeup(reqs, "water", [Supply(19.0, 72.8, units=4)], max_radius_km=50, limit=50)
    assert [r.id for r in one_event] == ["needs-3", "needs-1"]   # 3 + 1 = 4 units; 10 does not fit
    two_events = select_for_wakeup(reqs, "water", [Supply(19.0, 72.8, units=4), Supply(19.0, 72.8, units=10)],
                                   max_radius_km=50, limit=50)
    assert {r.id for r in two_events} == {"needs-10", "needs-3", "needs-1"}


# ---------------------------------------------------------------- supply detection (resource domain)
def make_resource(quantity=5):
    return rs.new_resource(rs.RegisterResource(type="food", name="depot", quantity=quantity,
                                               location=Location(lat=19, lon=72)))


def test_gained_supply():
    res = make_resource(5)
    offline = rs.update_availability(res, rs.ResourceStatus.OFFLINE, None)
    assert rs.gained_supply(offline, rs.update_availability(offline, rs.ResourceStatus.AVAILABLE, None))  # back online
    assert rs.gained_supply(res, rs.update_availability(res, None, 9))          # restocked
    assert not rs.gained_supply(res, rs.update_availability(res, None, 3))      # reduced
    assert not rs.gained_supply(res, offline)                                   # went offline


# ---------------------------------------------------------------- reconciler selection
def test_reconciler_picks_only_old_pending_not_recently_resent():
    from services.request_service.domain import stale_pending

    from types import SimpleNamespace as R

    now = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)
    iso = lambda dt: dt.isoformat().replace("+00:00", "Z")
    reqs = [
        R(id="old", status="PENDING", created_at=iso(now - timedelta(seconds=90))),
        R(id="young", status="PENDING", created_at=iso(now - timedelta(seconds=5))),
        R(id="resent", status="PENDING", created_at=iso(now - timedelta(seconds=90))),
        R(id="matched", status="ASSIGNED", created_at=iso(now - timedelta(seconds=90))),
    ]
    recently = {"resent": now - timedelta(seconds=10)}
    picked = stale_pending(reqs, now, min_age_s=30, recently_resent=recently, resend_every_s=60)
    assert [r.id for r in picked] == ["old"]

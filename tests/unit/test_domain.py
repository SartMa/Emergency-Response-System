"""Unit tests for pure domain logic: no network, no Docker. Run: uv run pytest tests/unit"""

import pytest

from services.common.errors import Conflict
from services.common.geo import haversine_km
from services.common.models import Location
from services.common.partitioner import NUM_PARTITIONS, Partitioner, partition_of
from services.matching_service.strategies import BalancedStrategy, NearestStrategy, RequestView, ResourceView
from services.request_service import domain as rq
from services.resource_service import domain as rs


# ---------------------------------------------------------------- partitioner
def test_partition_is_stable_and_in_range():
    assert partition_of("req:abc") == partition_of("req:abc")  # sha1, not randomised hash()
    assert all(0 <= partition_of(f"k{i}") < NUM_PARTITIONS for i in range(1000))


def test_keys_spread_evenly_over_shards():
    p = Partitioner()
    counts = [0, 0, 0]
    for i in range(3000):
        counts[p.shard_of(f"req:{i}")] += 1
    assert all(900 < c < 1100 for c in counts), counts


def test_fixed_partitions_move_fewer_keys_than_mod_n():
    """The ADR-03 argument, as a test: going from 3 to 4 shards."""
    keys = [f"req:{i}" for i in range(4000)]
    import hashlib

    def h(k):
        return int.from_bytes(hashlib.sha1(k.encode()).digest()[:8], "big")

    moved_mod = sum(h(k) % 3 != h(k) % 4 for k in keys) / len(keys)
    old = Partitioner({p: p % 3 for p in range(12)})
    new_map = dict(old.partition_map)
    for p in (0, 4, 8):  # hand one partition from each old shard to the new shard 3
        new_map[p] = 3
    new = Partitioner(new_map)
    moved_fixed = sum(old.shard_of(k) != new.shard_of(k) for k in keys) / len(keys)
    assert moved_mod > 0.7 and moved_fixed < 0.3


# ---------------------------------------------------------------- geo
def test_haversine_known_distance():
    # Mumbai CST -> Pune station is ~120 km in a straight line
    assert 115 < haversine_km(18.9400, 72.8350, 18.5285, 73.8743) < 125
    assert haversine_km(10, 10, 10, 10) == 0


# ---------------------------------------------------------------- request state machine
def make_request(**kw):
    body = rq.CreateRequest(type="water", location=Location(lat=19, lon=72), **kw)
    return rq.new_request(body)


def test_request_happy_path():
    r = make_request(quantity=2)
    r = rq.assign(r, rq.Assignment(resource_id="x", quantity=2, distance_km=1.5))
    r = rq.transition(r, rq.RequestStatus.IN_PROGRESS)
    r = rq.transition(r, rq.RequestStatus.FULFILLED)
    assert [h["status"] for h in r.history] == ["PENDING", "ASSIGNED", "IN_PROGRESS", "FULFILLED"]


def test_terminal_states_are_final():
    r = rq.transition(make_request(), rq.RequestStatus.CANCELLED)
    with pytest.raises(rq.InvalidTransition):
        rq.transition(r, rq.RequestStatus.IN_PROGRESS)


def test_cannot_assign_twice():
    r = rq.assign(make_request(), rq.Assignment(resource_id="x", quantity=1, distance_km=1))
    with pytest.raises(Conflict):
        rq.assign(r, rq.Assignment(resource_id="y", quantity=1, distance_km=1))


def test_unmatched_can_still_be_matched():
    r = rq.mark_unmatched(make_request())
    r = rq.mark_unmatched(r)
    assert r.status == "UNMATCHED" and r.match_attempts == 2
    assert rq.assign(r, rq.Assignment(resource_id="x", quantity=1, distance_km=1)).status == "ASSIGNED"


# ---------------------------------------------------------------- resource quantity
def make_resource(quantity=5):
    return rs.new_resource(rs.RegisterResource(type="food", name="depot", quantity=quantity,
                                               location=Location(lat=19, lon=72)))


def test_reserve_and_release_are_idempotent():
    res = make_resource(5)
    res, changed = rs.reserve(res, "req-1", 3)
    assert changed and res.availability.quantity_remaining == 2
    res2, changed = rs.reserve(res, "req-1", 3)  # redelivered message: no double take
    assert not changed and res2.availability.quantity_remaining == 2
    res, changed = rs.release(res, "req-1")
    assert changed and res.availability.quantity_remaining == 5 and res.allocations == []
    _, changed = rs.release(res, "req-1")
    assert not changed


def test_cannot_oversell_or_use_offline():
    res = make_resource(2)
    with pytest.raises(rs.InsufficientQuantity):
        rs.reserve(res, "req-1", 3)
    res, _ = rs.reserve(res, "req-1", 2)
    assert res.status == "EXHAUSTED"
    off = rs.update_availability(make_resource(), rs.ResourceStatus.OFFLINE, None)
    with pytest.raises(rs.NotAvailable):
        rs.reserve(off, "req-2", 1)


# ---------------------------------------------------------------- strategies
def test_nearest_filters_by_quantity_and_radius():
    req = RequestView("r", "water", 0.0, 0.0, quantity=4, urgency=3)
    resources = [
        ResourceView("tiny", "tiny", 0.01, 0.0, quantity_remaining=2, quantity_total=2),   # 1 km, too small
        ResourceView("ok", "ok", 0.05, 0.0, quantity_remaining=10, quantity_total=10),     # 5.6 km
        ResourceView("far", "far", 1.0, 0.0, quantity_remaining=10, quantity_total=10),    # 111 km, outside
    ]
    ranked = NearestStrategy().rank(req, resources, max_radius_km=50)
    assert [c.resource.id for c in ranked] == ["ok"]


def test_balanced_prefers_less_loaded_provider():
    req = RequestView("r", "water", 0.0, 0.0, quantity=1, urgency=3)
    nearly_empty = ResourceView("a", "a", 0.10, 0.0, quantity_remaining=1, quantity_total=10)  # 11 km, 90% used
    fresh = ResourceView("b", "b", 0.12, 0.0, quantity_remaining=10, quantity_total=10)       # 13 km, unused
    assert NearestStrategy().rank(req, [nearly_empty, fresh], 50)[0].resource.id == "a"
    assert BalancedStrategy().rank(req, [nearly_empty, fresh], 50)[0].resource.id == "b"

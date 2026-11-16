"""Unit tests for the storage node's store and the coordinator's cluster rules."""

import pytest

from services.coordinator.cluster import ClusterState, Health, Role, Sync, Timing
from services.storage_node.store import KVStore, VersionMismatch


@pytest.fixture
def store():
    s = KVStore(":memory:")
    yield s
    s.close()


# ---------------------------------------------------------------- KV store
def test_cas_create_update_conflict(store):
    assert store.put_primary("req:1", {"status": "PENDING"}, expected_version=0) == 1
    with pytest.raises(VersionMismatch):
        store.put_primary("req:1", {"status": "X"}, expected_version=0)  # already exists
    assert store.put_primary("req:1", {"status": "ASSIGNED"}, expected_version=1) == 2
    with pytest.raises(VersionMismatch):
        store.put_primary("req:1", {"status": "Y"}, expected_version=1)  # lost update prevented
    assert store.get("req:1").value == {"status": "ASSIGNED"}


def test_replica_applies_only_newer_versions(store):
    assert store.apply_replica("res:1", {"v": 2}, 2, False)
    assert not store.apply_replica("res:1", {"v": 1}, 1, False)  # arrived late, ignored
    assert store.get("res:1").version == 2


def test_tombstones_and_index(store):
    store.put_primary("res:a", {"type": "water", "status": "AVAILABLE"}, 0)
    store.put_primary("res:b", {"type": "food", "status": "AVAILABLE"}, 0)
    assert [r.key for r in store.query("res", "water", "AVAILABLE")] == ["res:a"]
    store.delete_primary("res:a", None)
    assert store.get("res:a") is None and store.get("res:a", include_deleted=True).deleted
    assert store.query("res", "water") == []
    assert "res:a" in store.digest()  # tombstone is part of the digest, so resync propagates deletes


# ---------------------------------------------------------------- cluster rules
def formed_cluster() -> tuple[ClusterState, float]:
    state = ClusterState(num_shards=1, now=0.0, timing=Timing(4, 6, 1))
    state.register("n0", 0, "http://n0", now=0.1)
    state.register("n1", 0, "http://n1", now=0.1)
    state.tick(now=2.0)  # forming ends -> election
    state.mark_in_sync("n1", now=2.0, pulled=0)
    state.heartbeat("n0", now=2.0)
    state.heartbeat("n1", now=2.0)
    return state, 2.0


def test_election_after_forming():
    state, _ = formed_cluster()
    assert state.primary[0] == "n0" and state.nodes["n0"].role == Role.PRIMARY


def test_heartbeat_timeout_promotes_in_sync_replica():
    state, t = formed_cluster()
    state.heartbeat("n1", now=t + 5)
    state.tick(now=t + 5)
    assert state.nodes["n0"].health == Health.SUSPECT
    state.heartbeat("n1", now=t + 7)
    state.tick(now=t + 7)  # n0 silent for 7 s > 6 s
    assert state.nodes["n0"].health == Health.DEAD
    assert state.primary[0] == "n1"
    assert state.nodes["n0"].sync == Sync.STALE  # must resync before it can be trusted again


def test_stale_replica_is_never_promoted():
    state, t = formed_cluster()
    state.replica_write_failed("n1", now=t)  # degraded mode: n1 missed a write
    state.failover(0, "n0", now=t, reason="test")
    assert state.primary[0] is None  # unavailable rather than serving stale data (CP)


def test_recovered_node_needs_sync_and_failover_is_idempotent():
    state, t = formed_cluster()
    state.failover(0, "n0", now=t, reason="connect error")
    state.failover(0, "n0", now=t, reason="second report of the same failure")
    assert state.primary[0] == "n1"
    state.heartbeat("n0", now=t + 1)
    assert [n.node_id for n in state.nodes_needing_sync()] == ["n0"]

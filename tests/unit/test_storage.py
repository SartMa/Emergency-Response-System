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
    state.mark_syncing("n1")  # the repair loop resyncs the replica...
    state.mark_in_sync("n1", now=2.0, pulled=0)  # ...and the digests matched
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


def test_stale_replica_never_wins_election_over_the_returning_primary():
    """Third bug from the chaos runs: the primary died while its replica was STALE; the next
    election picked the STALE replica (its reported version_sum looked high enough), and when
    the old primary came back it was resynced FROM the stale node, deleting acknowledged writes.
    Rule now: wait for the previous primary, which holds every acknowledged write."""
    state, t = formed_cluster()
    state.heartbeat("n0", now=t, version_sum=100)
    state.heartbeat("n1", now=t, version_sum=150)   # looks "ahead" (stale report from n0)...
    state.replica_write_failed("n1", now=t)         # ...but n1 is STALE: missing writes
    state.failover(0, "n0", now=t, reason="pod killed")
    assert state.primary[0] is None
    state.heartbeat("n1", now=t + 1)
    state.tick(now=t + 1)
    assert state.primary[0] is None                 # n1 must NOT be elected

    state.register("n0", 0, "http://n0", now=t + 3, version_sum=100)  # pod restarted, same volume
    state.tick(now=t + 3)
    assert state.primary[0] == "n0" and state.nodes["n1"].sync == Sync.STALE


def test_staleness_during_resync_is_not_overwritten():
    """Race found after a failed chaos run: a replica is resyncing (SYNCING) when it misses a live
    write (replica write failed), or is declared DEAD. Finishing the resync must NOT mark it
    IN_SYNC, because it is missing that write. It has to resync again."""
    state, t = formed_cluster()
    state.replica_write_failed("n1", now=t)     # n1 STALE
    state.mark_syncing("n1")                     # repair loop starts a resync
    state.replica_write_failed("n1", now=t + 1)  # ...and n1 misses a write meanwhile
    assert state.mark_in_sync("n1", now=t + 2, pulled=5) is False
    assert state.nodes["n1"].sync == Sync.STALE  # still not trustworthy
    state.heartbeat("n1", now=t + 2)             # healthy again -> the repair loop picks it up
    assert [n.node_id for n in state.nodes_needing_sync()] == ["n1"]

    # Same race with a heartbeat timeout during the resync (DEAD sets STALE).
    state.mark_syncing("n1")
    state.heartbeat("n0", now=t + 10)
    state.tick(now=t + 10)                       # n1 silent for > 6 s -> DEAD + STALE
    state.mark_in_sync("n1", now=t + 10, pulled=0)
    assert state.nodes["n1"].sync == Sync.STALE


def test_diverged_replica_is_demoted_and_resynced():
    """Anti-entropy safety net: an IN_SYNC replica that differs from its primary must not stay
    promotable. It becomes STALE, so the repair loop resyncs it and failover skips it."""
    state, t = formed_cluster()
    state.replica_diverged("n1", now=t, differing_keys=3)
    assert state.nodes["n1"].sync == Sync.STALE
    assert [n.node_id for n in state.nodes_needing_sync()] == ["n1"]
    state.replica_diverged("n0", now=t, differing_keys=3)  # the primary itself is never demoted this way
    assert state.nodes["n0"].sync == Sync.IN_SYNC and state.primary[0] == "n0"


def test_recovered_node_needs_sync_and_failover_is_idempotent():
    state, t = formed_cluster()
    state.failover(0, "n0", now=t, reason="connect error")
    state.failover(0, "n0", now=t, reason="second report of the same failure")
    assert state.primary[0] == "n1"
    state.heartbeat("n0", now=t + 1)
    assert [n.node_id for n in state.nodes_needing_sync()] == ["n0"]

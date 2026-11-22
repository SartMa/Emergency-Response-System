"""Cluster state: who is in each shard, who is primary, who is healthy, who is in sync.

Pure domain logic: no HTTP, no clocks (the caller passes `now`). That keeps the failover
rules unit-testable, and it is the part to read first when explaining the design.

Three independent attributes per node:
  role   PRIMARY | REPLICA              who serves reads and decides versions (single leader, ADR-05)
  health ALIVE | SUSPECT | DEAD          from heartbeats (spec §8) and from failed requests (ADR-07)
  sync   IN_SYNC | STALE | SYNCING       does it hold every acknowledged write? (DESIGN.md §5.5)

Invariant: only an ALIVE/SUSPECT node that is IN_SYNC may be promoted to primary. A STALE
node is missing writes, and promoting it would silently lose acknowledged data.
"""

from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum


class Role(StrEnum):
    PRIMARY = "PRIMARY"
    REPLICA = "REPLICA"


class Health(StrEnum):
    ALIVE = "ALIVE"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


class Sync(StrEnum):
    IN_SYNC = "IN_SYNC"
    STALE = "STALE"
    SYNCING = "SYNCING"


@dataclass
class NodeInfo:
    node_id: str
    shard_id: int
    url: str
    last_heartbeat: float
    role: Role = Role.REPLICA
    health: Health = Health.ALIVE
    sync: Sync = Sync.STALE
    key_counts: dict = field(default_factory=dict)
    version_sum: int = 0

    def usable(self) -> bool:
        return self.health != Health.DEAD

    def to_json(self, now: float) -> dict:
        return {
            "node_id": self.node_id,
            "shard_id": self.shard_id,
            "url": self.url,
            "role": self.role,
            "health": self.health,
            "sync": self.sync,
            "key_counts": self.key_counts,
            "keys": sum(self.key_counts.values()),
            "heartbeat_age_s": round(now - self.last_heartbeat, 1),
        }


@dataclass
class Timing:
    suspect_after_s: float = 4.0  # 2 missed heartbeats
    dead_after_s: float = 6.0  # 3 missed heartbeats (any timeout is a trade-off)
    forming_grace_s: float = 5.0  # after a coordinator (re)start, wait for nodes to re-register


class ClusterState:
    def __init__(self, num_shards: int, now: float, timing: Timing | None = None):
        self.num_shards = num_shards
        self.timing = timing or Timing()
        self.nodes: dict[str, NodeInfo] = {}
        self.primary: dict[int, str | None] = {s: None for s in range(num_shards)}
        # Shard -> the primary that failed while no replica was in sync. The shard waits for it.
        self.last_primary: dict[int, str | None] = {s: None for s in range(num_shards)}
        self.forming_until = now + self.timing.forming_grace_s
        self.events: deque[dict] = deque(maxlen=50)  # shown on the dashboard

    # ------------------------------------------------------------------ queries
    def is_forming(self, now: float) -> bool:
        return now < self.forming_until

    def shard_nodes(self, shard_id: int) -> list[NodeInfo]:
        return sorted((n for n in self.nodes.values() if n.shard_id == shard_id), key=lambda n: n.node_id)

    def primary_node(self, shard_id: int) -> NodeInfo | None:
        node_id = self.primary.get(shard_id)
        return self.nodes.get(node_id) if node_id else None

    def write_followers(self, shard_id: int) -> list[NodeInfo]:
        """Every non-primary node that is not DEAD receives replicated writes. STALE and SYNCING
        nodes get them too (best effort), so they are closer to current when the resync runs."""
        p = self.primary.get(shard_id)
        return [n for n in self.shard_nodes(shard_id) if n.node_id != p and n.usable()]

    def nodes_needing_sync(self) -> list[NodeInfo]:
        out = []
        for node in self.nodes.values():
            primary = self.primary_node(node.shard_id)
            if (
                node.sync == Sync.STALE
                and node.health == Health.ALIVE
                and primary is not None
                and primary.node_id != node.node_id
                and primary.usable()
            ):
                out.append(node)
        return out

    # ------------------------------------------------------------------ events
    def _event(self, now: float, kind: str, message: str) -> None:
        self.events.appendleft({"t": now, "kind": kind, "message": message})

    # ------------------------------------------------------------------ membership
    def register(self, node_id: str, shard_id: int, url: str, now: float,
                 key_counts: dict | None = None, version_sum: int = 0) -> NodeInfo:
        """A node (re)starts. A restarted node may have missed writes while it was down, so it
        starts as a STALE replica and is resynced, unless it is the shard's only source of truth."""
        if not 0 <= shard_id < self.num_shards:
            raise ValueError(f"shard_id {shard_id} out of range")
        old = self.nodes.get(node_id)
        node = NodeInfo(node_id, shard_id, url, last_heartbeat=now,
                        key_counts=key_counts or {}, version_sum=version_sum)
        self.nodes[node_id] = node
        self._event(now, "register", f"{node_id} registered with shard {shard_id}"
                    + (" (restart)" if old else ""))

        if self.primary[shard_id] == node_id:
            # The primary itself restarted before we noticed it was gone. Hand over to an
            # in-sync replica if there is one, otherwise it stays primary (it has the data).
            if not self._promote_in_sync_replica(shard_id, exclude=node_id, now=now):
                node.role, node.sync = Role.PRIMARY, Sync.IN_SYNC
        # Shards without a primary get one in tick() -> _elect_missing_primaries().
        return node

    def heartbeat(self, node_id: str, now: float, key_counts: dict | None = None,
                  version_sum: int | None = None) -> bool:
        node = self.nodes.get(node_id)
        if node is None:
            return False  # unknown: the node must register (we probably restarted)
        node.last_heartbeat = now
        if key_counts is not None:
            node.key_counts = key_counts
        if version_sum is not None:
            node.version_sum = version_sum
        if node.health != Health.ALIVE:
            # Coming back from SUSPECT/DEAD. A node that was DEAD was already marked STALE,
            # so the repair loop will resync it before it is trusted again.
            self._event(now, "alive", f"{node_id} is ALIVE again (was {node.health})")
            node.health = Health.ALIVE
        return True

    # ------------------------------------------------------------------ periodic tick
    def tick(self, now: float) -> None:
        if not self.is_forming(now):
            self._elect_missing_primaries(now)

        for node in list(self.nodes.values()):
            age = now - node.last_heartbeat
            if age > self.timing.dead_after_s and node.health != Health.DEAD:
                node.health = Health.DEAD
                node.sync = Sync.STALE  # it will miss writes from now on
                self._event(now, "dead", f"{node.node_id} declared DEAD (no heartbeat for {age:.1f}s)")
                if self.primary[node.shard_id] == node.node_id:
                    self.failover(node.shard_id, node.node_id, now, reason="heartbeat timeout")
            elif self.timing.suspect_after_s < age <= self.timing.dead_after_s and node.health == Health.ALIVE:
                node.health = Health.SUSPECT
                self._event(now, "suspect", f"{node.node_id} is SUSPECT (no heartbeat for {age:.1f}s)")

    def _elect_missing_primaries(self, now: float) -> None:
        """Give every shard without a primary a new one, if that is safe.

        Case 1: the primary failed while no replica was in sync (failover() remembered it in
        last_primary). Only that node is known to hold every acknowledged write, so we wait for
        it and re-elect it when it is back. Electing anyone else could lose acknowledged writes:
        a chaos run showed a STALE replica winning a version_sum comparison against the
        returning primary's slightly out-of-date report, after which the primary was resynced
        FROM the stale node and acknowledged writes were deleted. This is the
        consistency-over-availability choice (CP).

        Case 2: we have no memory (just after a coordinator restart, soft state ADR-14). Pick
        the live node that has seen the most writes (highest version_sum), unless a DEAD node
        reported more. That is the best guess available without a persistent log."""
        for shard_id in range(self.num_shards):
            if self.primary[shard_id] is not None:
                continue
            nodes = self.shard_nodes(shard_id)

            waiting_for = self.last_primary.get(shard_id)
            if waiting_for is not None:  # case 1
                node = self.nodes.get(waiting_for)
                if node is not None and node.health == Health.ALIVE:
                    self._make_primary(node, now, reason="previous primary is back; it holds every acknowledged write")
                    self.last_primary[shard_id] = None
                    for other in nodes:
                        if other is not node:
                            other.role, other.sync = Role.REPLICA, Sync.STALE
                continue

            # case 2
            candidates = [n for n in nodes if n.usable()]
            if not candidates:
                continue
            best = max(candidates, key=lambda n: n.version_sum)  # ties: lowest node_id (list is sorted)
            if any(n.version_sum > best.version_sum for n in nodes if not n.usable()):
                continue
            self._make_primary(best, now, reason=f"elected, version_sum={best.version_sum}")
            for other in candidates:
                if other is not best:
                    other.role, other.sync = Role.REPLICA, Sync.STALE

    # ------------------------------------------------------------------ failover
    def failover(self, shard_id: int, failed_node_id: str, now: float, reason: str) -> NodeInfo | None:
        """The primary of `shard_id` is gone or failing. Promote the in-sync replica if there
        is one. Called by tick() (heartbeat timeout) and by the router the moment a request
        to the primary fails (fast failover, ADR-07). Safe to call twice for the same failure."""
        if self.primary.get(shard_id) != failed_node_id:
            return self.primary_node(shard_id)  # someone already handled this failure
        failed = self.nodes[failed_node_id]
        failed.role = Role.REPLICA
        failed.sync = Sync.STALE  # it may hold writes the new primary does not have (discarded on resync)
        if failed.health == Health.ALIVE:
            failed.health = Health.SUSPECT
        self._event(now, "failover", f"primary {failed_node_id} of shard {shard_id} failed ({reason})")
        promoted = self._promote_in_sync_replica(shard_id, exclude=failed_node_id, now=now)
        if promoted is None:
            self.primary[shard_id] = None
            # Remember who it was: that node (with its persistent volume) holds every acknowledged
            # write, so the shard waits for it instead of electing a node that is missing writes.
            self.last_primary[shard_id] = failed_node_id
            self._event(now, "unavailable", f"shard {shard_id} has NO in-sync replica: unavailable (503) "
                                            f"until {failed_node_id} is back")
        return promoted

    def replica_write_failed(self, node_id: str, now: float) -> None:
        """Synchronous replication to this replica failed. The write was still acknowledged
        (degraded mode, DESIGN.md §5.3); the replica is STALE until resynced."""
        node = self.nodes.get(node_id)
        if node and self.primary.get(node.shard_id) == node_id:
            return  # the primary itself: that's a failover matter (handled on its next failed request)
        if node and node.sync != Sync.STALE:
            node.sync = Sync.STALE
            if node.health == Health.ALIVE:
                node.health = Health.SUSPECT
            self._event(now, "stale", f"{node_id} missed a replicated write, now STALE (degraded mode)")

    def replica_diverged(self, node_id: str, now: float, differing_keys: int) -> None:
        """Anti-entropy found that an IN_SYNC replica does not match its primary. It must not be
        promoted while it is missing data, so it becomes STALE and the repair loop resyncs it."""
        node = self.nodes.get(node_id)
        if node and node.sync == Sync.IN_SYNC and self.primary.get(node.shard_id) != node_id:
            node.sync = Sync.STALE
            self._event(now, "diverged", f"anti-entropy: {node_id} differs from its primary in "
                                         f"{differing_keys} key(s); marked STALE for resync")

    def _promote_in_sync_replica(self, shard_id: int, exclude: str, now: float) -> NodeInfo | None:
        for node in self.shard_nodes(shard_id):
            if node.node_id != exclude and node.usable() and node.sync == Sync.IN_SYNC:
                self._make_primary(node, now, reason="promoted in-sync replica")
                old = self.nodes.get(exclude)
                if old:
                    old.role, old.sync = Role.REPLICA, Sync.STALE
                return node
        return None

    def _make_primary(self, node: NodeInfo, now: float, reason: str) -> None:
        for other in self.shard_nodes(node.shard_id):
            if other.role == Role.PRIMARY and other is not node:
                other.role = Role.REPLICA
        node.role, node.sync = Role.PRIMARY, Sync.IN_SYNC
        self.primary[node.shard_id] = node.node_id
        self._event(now, "primary", f"{node.node_id} is now PRIMARY of shard {node.shard_id} ({reason})")

    # ------------------------------------------------------------------ resync bookkeeping
    def mark_syncing(self, node_id: str) -> None:
        self.nodes[node_id].sync = Sync.SYNCING

    def mark_in_sync(self, node_id: str, now: float, pulled: int) -> bool:
        """A resync finished and the digests matched. Only trust the node if it is still SYNCING.
        If something marked it STALE meanwhile (a live replica write to it failed, or it was
        declared DEAD), it is missing at least one write, so it must resync again.

        Bug found after a failed chaos run: this used to set IN_SYNC unconditionally, overwriting
        that STALE. The result was a replica marked IN_SYNC that silently lacked writes, and a
        failover to it lost them."""
        node = self.nodes[node_id]
        if node.sync != Sync.SYNCING:
            self._event(now, "stale", f"{node_id} resync finished but it missed writes meanwhile; resyncing again")
            return False
        node.sync = Sync.IN_SYNC
        self._event(now, "in_sync", f"{node_id} resynced ({pulled} keys copied), now IN_SYNC replica")
        return True

    def mark_sync_failed(self, node_id: str) -> None:
        node = self.nodes.get(node_id)
        if node and node.sync == Sync.SYNCING:
            node.sync = Sync.STALE

    # ------------------------------------------------------------------ view
    def to_json(self, now: float) -> dict:
        return {
            "forming": self.is_forming(now),
            "shards": [
                {
                    "shard_id": s,
                    "primary": self.primary[s],
                    "available": self.primary_node(s) is not None and self.primary_node(s).usable(),
                    "nodes": [n.to_json(now) for n in self.shard_nodes(s)],
                }
                for s in range(self.num_shards)
            ],
            "events": [{**e, "age_s": round(now - e["t"], 1)} for e in self.events],
        }

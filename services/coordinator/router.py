"""The coordinator's I/O side: routes reads/writes to shards, replicates, fails over, resyncs.

Write path (single leader, synchronous replication, ADR-05):
    client -> coordinator -> PRIMARY (compare-and-set, assigns version)
                          -> every follower (same value + version)
                          -> ack to client
Read path: PRIMARY only (read-your-writes, NFR-3).
On a transport error to the primary: fail over at once and retry once (ADR-07).
"""

import asyncio
import logging
import time

import httpx

from services.common.errors import NotFound, Unavailable, VersionConflict
from services.common.http import make_client
from services.common.partitioner import Partitioner
from services.coordinator.cluster import ClusterState, NodeInfo, Sync

log = logging.getLogger("router")


def now() -> float:
    return time.monotonic()


class Router:
    def __init__(self, state: ClusterState, partitioner: Partitioner):
        self.state = state
        self.partitioner = partitioner
        # Short timeouts: a dead node must be noticed in well under a second, not after 30 s.
        self.http = make_client(timeout=2.0)
        self._syncing: set[str] = set()
        self._divergent_last_round: dict[str, set[tuple]] = {}  # anti-entropy: node -> (key, version) diffs last round

    async def close(self) -> None:
        await self.http.aclose()

    # ------------------------------------------------------------------ helpers
    def _primary_or_503(self, shard_id: int) -> NodeInfo:
        if self.state.is_forming(now()):
            raise Unavailable("Cluster is forming after a coordinator restart; retry in a few seconds",
                              code="CLUSTER_FORMING")
        node = self.state.primary_node(shard_id)
        if node is None or not node.usable():
            raise Unavailable(f"Shard {shard_id} has no available in-sync node", code="SHARD_UNAVAILABLE")
        return node

    async def _on_primary(self, shard_id: int, call, on_timeout=None):
        """Run `call(primary_node)`. If the primary is unreachable, fail over and retry once.

        A *slow* primary is different from a dead one. A connection that is refused or reset
        (a killed pod) means dead: fail over immediately (ADR-07). A read timeout on an open
        connection only means "no answer yet": the node is probably alive, just busy. Failing
        it over then was a guess that went wrong under load (a chaos run saw 21 failovers for
        15 kills, each one causing a resync and even more load). So on a read timeout we don't
        fail over (heartbeats still catch a node that really hangs). We give `on_timeout` a
        chance to clean up a write whose outcome is unknown, then answer 503 so the client retries."""

        def is_slow(exc: Exception) -> bool:
            return isinstance(exc, httpx.TimeoutException) and not isinstance(exc, httpx.ConnectTimeout)

        async def slow(node: NodeInfo, exc: Exception):
            log.warning(f"primary {node.node_id} did not answer in time ({exc.__class__.__name__}); not failing over")
            if on_timeout is not None:
                await on_timeout(node)
            raise Unavailable(f"Shard {shard_id}: primary did not answer in time; retry", code="PRIMARY_TIMEOUT")

        primary = self._primary_or_503(shard_id)
        try:
            return await call(primary)
        except httpx.TransportError as exc:
            if is_slow(exc):
                await slow(primary, exc)
            log.warning(f"primary {primary.node_id} unreachable ({exc.__class__.__name__}); failing over",
                        extra={"fields": {"shard": shard_id}})
            self.state.failover(shard_id, primary.node_id, now(), reason=exc.__class__.__name__)
            new_primary = self._primary_or_503(shard_id)
            try:
                return await call(new_primary)
            except httpx.TransportError as exc2:
                if is_slow(exc2):
                    await slow(new_primary, exc2)
                self.state.failover(shard_id, new_primary.node_id, now(), reason=exc2.__class__.__name__)
                raise Unavailable(f"Shard {shard_id}: primary and replica unreachable", code="SHARD_UNAVAILABLE")

    async def _repair_after_timeout(self, shard_id: int, node: NodeInfo, key: str) -> None:
        """A write to `node` timed out: it may or may not have been applied. Read back what the
        node holds for `key` now and replicate exactly that ("read repair"), so a write that did
        land is never left on the primary alone. If even the read-back fails, mark the followers
        STALE: they might be missing a write, so they must not be promoted until resynced."""
        try:
            r = await self.http.get(f"{node.url}/internal/data/{key}", params={"include_deleted": "true"},
                                    timeout=5.0)
            if r.status_code == 404:
                return  # the write never landed: nothing to repair
            r.raise_for_status()
            rec = r.json()
            await self._replicate(shard_id, node, key, rec["value"], rec["version"], rec["deleted"])
            log.info(f"read repair after timeout: replicated {key} v{rec['version']} from {node.node_id}")
        except Exception as exc:
            log.warning(f"read repair of {key} failed ({exc.__class__.__name__}); marking followers STALE")
            for follower in self.state.shard_nodes(shard_id):
                if follower.node_id != node.node_id:
                    self.state.replica_write_failed(follower.node_id, now())

    # ------------------------------------------------------------------ KV operations
    async def get(self, key: str) -> dict:
        shard_id = self.partitioner.shard_of(key)

        async def call(node: NodeInfo):
            return await self.http.get(f"{node.url}/internal/data/{key}")

        r = await self._on_primary(shard_id, call)
        if r.status_code == 404:
            raise NotFound(f"Key {key} not found")
        r.raise_for_status()
        body = r.json()
        return {"key": key, "value": body["value"], "version": body["version"]}

    async def _uninterruptible(self, coro, what: str):
        """Run a write (primary write + replication) to completion even if our caller goes away.

        Defensive: if a request handler were ever cancelled after the primary write but before
        replication, the replica would silently miss the write while counting as IN_SYNC.
        asyncio.shield lets the caller be cancelled while the write itself keeps running.
        (An A/B test with 300 clients hanging up mid-write showed no cancellations with the
        current framework versions; this guards against that changing.)"""
        task = asyncio.ensure_future(coro)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            def report(t: asyncio.Task) -> None:
                exc = None if t.cancelled() else t.exception()  # retrieving it avoids a "never retrieved" warning
                log.info(f"{what}: caller disconnected; the write was still completed"
                         + (f" with error {exc!r}" if exc else ""))
            task.add_done_callback(report)
            raise

    async def put(self, key: str, value: dict, expected_version: int | None) -> int:
        return await self._uninterruptible(self._put(key, value, expected_version), f"PUT {key}")

    async def delete(self, key: str, expected_version: int | None) -> int:
        return await self._uninterruptible(self._delete(key, expected_version), f"DELETE {key}")

    async def _put(self, key: str, value: dict, expected_version: int | None) -> int:
        shard_id = self.partitioner.shard_of(key)

        served: dict[str, NodeInfo] = {}  # which node actually performed the write

        async def call(node: NodeInfo):
            served["writer"] = node
            return await self.http.put(f"{node.url}/internal/data/{key}",
                                       json={"value": value, "expected_version": expected_version})

        r = await self._on_primary(shard_id, call,
                                   on_timeout=lambda node: self._repair_after_timeout(shard_id, node, key))
        if r.status_code == 409:
            details = r.json().get("error", {}).get("details", {})
            raise VersionConflict(f"Version conflict on {key}", details=details)
        r.raise_for_status()
        version = r.json()["version"]
        await self._replicate(shard_id, served["writer"], key, value, version, deleted=False)
        return version

    async def _delete(self, key: str, expected_version: int | None) -> int:
        shard_id = self.partitioner.shard_of(key)

        served: dict[str, NodeInfo] = {}

        async def call(node: NodeInfo):
            served["writer"] = node
            params = {} if expected_version is None else {"expected_version": expected_version}
            return await self.http.delete(f"{node.url}/internal/data/{key}", params=params)

        r = await self._on_primary(shard_id, call,
                                   on_timeout=lambda node: self._repair_after_timeout(shard_id, node, key))
        if r.status_code == 404:
            raise NotFound(f"Key {key} not found")
        if r.status_code == 409:
            raise VersionConflict(f"Version conflict on {key}")
        r.raise_for_status()
        version = r.json()["version"]
        await self._replicate(shard_id, served["writer"], key, None, version, deleted=True)
        return version

    async def _replicate(self, shard_id: int, writer: NodeInfo, key: str, value, version: int,
                         deleted: bool) -> None:
        """Synchronous replication: send the write to every other usable node of the shard and
        wait for them before acknowledging the client. A follower that fails is marked STALE and
        the write still succeeds (degraded mode).

        Race fixed after a chaos run: a concurrent request can trigger a failover WHILE this write
        is in progress. The write was applied on `writer` (the old primary), but replication used
        to skip "the current primary", which is now the NEW primary, the one node that needed it.
        The write then lived only on the dead old primary, was acknowledged anyway, and was deleted
        when that node resynced from the new primary. Now we (1) replicate to every node except
        the one that actually performed the write, and (2) refuse to acknowledge (503, the client
        retries) unless the node that is primary NOW confirms it applied the write."""
        followers = [n for n in self.state.shard_nodes(shard_id) if n.node_id != writer.node_id and n.usable()]
        body = {"key": key, "value": value, "version": version, "deleted": deleted}
        applied: dict[str, bool | None] = {}

        async def send(node: NodeInfo):
            try:
                r = await self.http.post(f"{node.url}/internal/replicate", json=body)
                r.raise_for_status()
                applied[node.node_id] = bool(r.json().get("applied", True))
            except Exception as exc:  # ANY failure: a replica we can't vouch for must not stay IN_SYNC
                log.warning(f"replication to {node.node_id} failed ({exc.__class__.__name__})")
                self.state.replica_write_failed(node.node_id, now())
                applied[node.node_id] = None

        await asyncio.gather(*(send(n) for n in followers))

        current = self.state.primary_node(shard_id)
        if current is None:
            raise Unavailable(f"Shard {shard_id} lost its primary during the write; not acknowledged, retry",
                              code="PRIMARY_CHANGED")
        if current.node_id != writer.node_id and applied.get(current.node_id) is not True:
            log.warning(f"primary of shard {shard_id} changed during write of {key} "
                        f"({writer.node_id} -> {current.node_id}); not acknowledging")
            raise Unavailable(f"Primary of shard {shard_id} changed during the write; not acknowledged, retry",
                              code="PRIMARY_CHANGED")

    async def query(self, kind: str, type_: str | None, status: str | None) -> dict:
        """Scatter-gather: ask every shard's primary in parallel and merge the answers (DDIA ch. 7,
        "local secondary indexes"). If a shard cannot answer we still return the others, with
        partial=True, rather than failing the whole query (NFR-11)."""
        params = {"kind": kind}
        if type_:
            params["type"] = type_
        if status:
            params["status"] = status

        async def one_shard(shard_id: int):
            async def call(node: NodeInfo):
                return await self.http.get(f"{node.url}/internal/query", params=params)

            try:
                r = await self._on_primary(shard_id, call)
                r.raise_for_status()
                return shard_id, r.json()["items"]
            except (Unavailable, httpx.HTTPError) as exc:
                log.warning(f"query: shard {shard_id} unavailable ({exc.__class__.__name__})")
                return shard_id, None

        results = await asyncio.gather(*(one_shard(s) for s in range(self.state.num_shards)))
        items, missing = [], []
        for shard_id, shard_items in results:
            if shard_items is None:
                missing.append(shard_id)
            else:
                items.extend({"key": i["key"], "value": i["value"], "version": i["version"]} for i in shard_items)
        return {"items": items, "partial": bool(missing), "missing_shards": missing}

    # ------------------------------------------------------------------ consistency check
    async def verify(self) -> dict:
        """Compare every node's digest with its shard primary's: are the copies identical?
        Used by the chaos test and the dashboard to *prove* replica convergence, not just claim it.
        A node that is still syncing (or receiving an in-flight write) may briefly differ."""

        async def digest(node: NodeInfo):
            try:
                r = await self.http.get(f"{node.url}/internal/digest", timeout=10.0)
                r.raise_for_status()
                return r.json()["digest"]
            except httpx.HTTPError:
                return None

        shards = []
        for shard_id in range(self.state.num_shards):
            primary = self.state.primary_node(shard_id)
            nodes = [n for n in self.state.shard_nodes(shard_id) if n.usable()]
            digests = await asyncio.gather(*(digest(n) for n in nodes))
            by_id = dict(zip([n.node_id for n in nodes], digests))
            reference = by_id.get(primary.node_id) if primary else None
            report = []
            for node in nodes:
                d = by_id[node.node_id]
                if d is None or reference is None:
                    report.append({"node_id": node.node_id, "sync": node.sync, "identical": None})
                    continue
                diff = [k for k in reference.keys() | d.keys() if reference.get(k) != d.get(k)]
                report.append({"node_id": node.node_id, "sync": node.sync, "identical": not diff,
                               "differing_keys": len(diff),
                               # (key, primary's version) for up to 200 differences: lets anti-entropy
                               # tell a write in flight apart from a copy that is really wrong
                               "diff_sample": [[k, (reference.get(k) or [None])[0]] for k in sorted(diff)[:200]]})
            shards.append({
                "shard_id": shard_id,
                "primary": primary.node_id if primary else None,
                "keys": len(reference) if reference is not None else None,
                "nodes": report,
                "consistent": reference is not None and all(n["identical"] for n in report),
            })
        return {"consistent": all(s["consistent"] for s in shards), "shards": shards}

    async def anti_entropy_once(self) -> None:
        """Background anti-entropy (the approach Dynamo and Cassandra use): periodically compare
        every IN_SYNC replica with its primary. Normally they are identical, since the write path
        replicates synchronously. This is the safety net for any bug or race we have not thought
        of.

        A difference seen once may just be a write in flight between primary and replica. Under
        constant writes there is almost always *some* key in flight, so "differs in two rounds"
        is not enough (a chaos run showed that rule forcing endless resyncs). A replica is only
        marked STALE if the SAME key, at the SAME primary version, differs in two rounds in a
        row: the primary did not change that key in between, and the replica still lacks it."""
        report = await self.verify()
        seen_now: dict[str, set[tuple]] = {}
        for shard in report["shards"]:
            for n in shard["nodes"]:
                node = self.state.nodes.get(n["node_id"])
                if (n["identical"] is False and node is not None and node.sync == Sync.IN_SYNC
                        and shard["primary"] != node.node_id):
                    diffs = {tuple(x) for x in n.get("diff_sample", [])}
                    seen_now[node.node_id] = diffs
                    persistent = diffs & self._divergent_last_round.get(node.node_id, set())
                    if persistent:
                        log.warning(f"anti-entropy: {node.node_id} still lacks {len(persistent)} key version(s)")
                        self.state.replica_diverged(node.node_id, now(), len(persistent))
        self._divergent_last_round = seen_now

    # ------------------------------------------------------------------ resync (repair loop)
    async def repair_once(self) -> None:
        for node in self.state.nodes_needing_sync():
            if node.node_id not in self._syncing:
                self._syncing.add(node.node_id)
                asyncio.create_task(self._resync(node.node_id))

    async def _resync(self, node_id: str) -> None:
        """Catch-up recovery (DESIGN.md §5.5): the node pulls the keys that differ from the primary,
        then we compare both digests. Only when they are identical does the node become IN_SYNC
        and therefore eligible for promotion again. Live writes keep flowing to it meanwhile; if
        one raced with the sync, the digests differ and we simply sync again."""
        try:
            self.state.mark_syncing(node_id)
            node = self.state.nodes[node_id]
            for attempt in range(1, 6):
                primary = self.state.primary_node(node.shard_id)
                if primary is None or primary.node_id == node_id:
                    self.state.mark_sync_failed(node_id)
                    return
                r = await self.http.post(f"{node.url}/internal/sync", json={"source_url": primary.url},
                                         timeout=60.0)
                r.raise_for_status()
                pulled = r.json()["pulled"]
                d_node, d_primary = await asyncio.gather(
                    self.http.get(f"{node.url}/internal/digest"),
                    self.http.get(f"{primary.url}/internal/digest"),
                )
                if d_node.json()["digest"] == d_primary.json()["digest"]:
                    self.state.mark_in_sync(node_id, now(), pulled)
                    return
                log.info(f"resync of {node_id}: digests still differ (attempt {attempt}), syncing again")
            self.state.mark_sync_failed(node_id)
        except Exception as exc:
            log.warning(f"resync of {node_id} failed: {exc!r}")
            self.state.mark_sync_failed(node_id)
        finally:
            self._syncing.discard(node_id)

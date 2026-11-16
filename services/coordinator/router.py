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
from services.coordinator.cluster import ClusterState, NodeInfo

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

    async def _on_primary(self, shard_id: int, call):
        """Run `call(primary_node)`; if the primary is unreachable, fail over and retry once."""
        primary = self._primary_or_503(shard_id)
        try:
            return await call(primary)
        except httpx.TransportError as exc:
            log.warning(f"primary {primary.node_id} unreachable ({exc.__class__.__name__}); failing over",
                        extra={"fields": {"shard": shard_id}})
            self.state.failover(shard_id, primary.node_id, now(), reason=exc.__class__.__name__)
            new_primary = self._primary_or_503(shard_id)
            try:
                return await call(new_primary)
            except httpx.TransportError as exc2:
                self.state.failover(shard_id, new_primary.node_id, now(), reason=exc2.__class__.__name__)
                raise Unavailable(f"Shard {shard_id}: primary and replica unreachable", code="SHARD_UNAVAILABLE")

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

    async def put(self, key: str, value: dict, expected_version: int | None) -> int:
        shard_id = self.partitioner.shard_of(key)

        async def call(node: NodeInfo):
            return await self.http.put(f"{node.url}/internal/data/{key}",
                                       json={"value": value, "expected_version": expected_version})

        r = await self._on_primary(shard_id, call)
        if r.status_code == 409:
            details = r.json().get("error", {}).get("details", {})
            raise VersionConflict(f"Version conflict on {key}", details=details)
        r.raise_for_status()
        version = r.json()["version"]
        await self._replicate(shard_id, key, value, version, deleted=False)
        return version

    async def delete(self, key: str, expected_version: int | None) -> int:
        shard_id = self.partitioner.shard_of(key)

        async def call(node: NodeInfo):
            params = {} if expected_version is None else {"expected_version": expected_version}
            return await self.http.delete(f"{node.url}/internal/data/{key}", params=params)

        r = await self._on_primary(shard_id, call)
        if r.status_code == 404:
            raise NotFound(f"Key {key} not found")
        if r.status_code == 409:
            raise VersionConflict(f"Version conflict on {key}")
        r.raise_for_status()
        version = r.json()["version"]
        await self._replicate(shard_id, key, None, version, deleted=True)
        return version

    async def _replicate(self, shard_id: int, key: str, value, version: int, deleted: bool) -> None:
        """Synchronous replication: wait for every follower before acknowledging the client.
        A follower that fails is marked STALE and the write still succeeds (degraded mode)."""
        followers = self.state.write_followers(shard_id)
        if not followers:
            return
        body = {"key": key, "value": value, "version": version, "deleted": deleted}

        async def send(node: NodeInfo):
            try:
                r = await self.http.post(f"{node.url}/internal/replicate", json=body)
                r.raise_for_status()
            except httpx.HTTPError as exc:
                log.warning(f"replication to {node.node_id} failed ({exc.__class__.__name__})")
                self.state.replica_write_failed(node.node_id, now())

        await asyncio.gather(*(send(n) for n in followers))

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
                differing = sum(1 for k in reference.keys() | d.keys() if reference.get(k) != d.get(k))
                report.append({"node_id": node.node_id, "sync": node.sync, "identical": differing == 0,
                               "differing_keys": differing})
            shards.append({
                "shard_id": shard_id,
                "primary": primary.node_id if primary else None,
                "keys": len(reference) if reference is not None else None,
                "nodes": report,
                "consistent": reference is not None and all(n["identical"] for n in report),
            })
        return {"consistent": all(s["consistent"] for s in shards), "shards": shards}

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

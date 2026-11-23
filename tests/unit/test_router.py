"""Coordinator write path against simulated storage nodes (httpx MockTransport, no network).

Regression test for the race found by a chaos run: a failover triggered by a concurrent request
while a write is in progress. The write was applied on the old primary only and still
acknowledged; when the old primary later resynced from the new one, the write was deleted."""

import json
import time

import httpx
import pytest

from services.common.errors import Unavailable
from services.common.partitioner import Partitioner
from services.coordinator.cluster import ClusterState, Timing
from services.coordinator.router import Router


def build_cluster() -> ClusterState:
    t0 = time.monotonic()
    state = ClusterState(num_shards=1, now=t0 - 10, timing=Timing(4, 6, forming_grace_s=0))
    state.register("n0", 0, "http://n0", now=t0)
    state.register("n1", 0, "http://n1", now=t0)
    state.tick(now=t0)  # elects n0 (lowest id)
    state.mark_syncing("n1")
    state.mark_in_sync("n1", now=t0, pulled=0)
    return state


def make_router(state: ClusterState, failover_during_put: bool, new_primary_down: bool = False):
    stores: dict[str, dict] = {"n0": {}, "n1": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        node = request.url.host
        if node == "n1" and new_primary_down and request.url.path == "/internal/replicate":
            raise httpx.ConnectError("n1 unreachable")
        if request.method == "PUT":  # primary write on n0
            key = request.url.path.rsplit("/", 1)[-1]
            stores[node][key] = json.loads(request.content)["value"]
            if failover_during_put:
                # Meanwhile another request found n0 dead and failed over to n1.
                state.failover(0, "n0", time.monotonic(), reason="concurrent request saw n0 die")
            return httpx.Response(200, json={"key": key, "version": 1})
        if request.url.path == "/internal/replicate":
            body = json.loads(request.content)
            stores[node][body["key"]] = body["value"]
            return httpx.Response(200, json={"applied": True})
        return httpx.Response(404)

    router = Router(state, Partitioner({p: 0 for p in range(12)}))
    router.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return router, stores


async def test_timed_out_write_is_read_repaired_without_failover():
    """The primary applied the write but its answer timed out (busy, not dead). We must not fail
    a live node over on a guess, and the write that did land must reach the replica."""
    state = build_cluster()
    stores: dict[str, dict] = {"n0": {}, "n1": {}}

    def handler(request: httpx.Request) -> httpx.Response:
        node = request.url.host
        if request.method == "PUT":
            stores[node]["req:a"] = json.loads(request.content)["value"]
            raise httpx.ReadTimeout("answer lost", request=request)
        if request.method == "GET":  # read-back
            return httpx.Response(200, json={"key": "req:a", "value": stores[node]["req:a"], "version": 1, "deleted": False})
        if request.url.path == "/internal/replicate":
            stores[node]["req:a"] = json.loads(request.content)["value"]
            return httpx.Response(200, json={"applied": True})
        return httpx.Response(404)

    router = Router(state, Partitioner({p: 0 for p in range(12)}))
    router.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(Unavailable):  # outcome unknown to the client: 503, it retries
        await router.put("req:a", {"x": 1}, expected_version=0)
    assert state.primary[0] == "n0"                 # no failover of a node that was only slow
    assert stores["n1"].get("req:a") == {"x": 1}    # the write that landed is replicated


async def test_normal_write_is_replicated():
    state = build_cluster()
    router, stores = make_router(state, failover_during_put=False)
    assert await router.put("req:a", {"x": 1}, expected_version=0) == 1
    assert "req:a" in stores["n0"] and "req:a" in stores["n1"]


async def test_write_during_failover_reaches_the_new_primary():
    state = build_cluster()
    router, stores = make_router(state, failover_during_put=True)
    await router.put("req:a", {"x": 1}, expected_version=0)
    assert state.primary[0] == "n1"
    assert "req:a" in stores["n1"]  # the new primary has it, so the acknowledgement is honest


async def test_write_during_failover_is_not_acknowledged_if_new_primary_missed_it():
    state = build_cluster()
    router, _ = make_router(state, failover_during_put=True, new_primary_down=True)
    with pytest.raises(Unavailable):  # 503: the client retries, and no false promise is made
        await router.put("req:a", {"x": 1}, expected_version=0)

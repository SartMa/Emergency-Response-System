"""Coordinator (spec §4, §6, §8): the single entry point to distributed storage.

Endpoints (internal, never exposed through the Ingress):
  POST /internal/nodes/register | /internal/nodes/heartbeat   membership (spec Table 3)
  PUT|GET|DELETE /internal/data/{key}                         routed KV with versions
  GET /internal/query                                         scatter-gather across shards
  GET /internal/cluster  /internal/locate/{key}  /dashboard   observability (FR-21, FR-22)

The cluster map is soft state, rebuilt from heartbeats after a restart (ADR-14).
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from services.common.config import env_float, env_int, env_str
from services.common.errors import NotFound
from services.common.http import setup_app
from services.common.logging import setup_logging
from services.common.partitioner import NUM_SHARDS, Partitioner, default_partition_map, partition_of
from services.coordinator.cluster import ClusterState, Timing
from services.coordinator.router import Router, now

log = setup_logging("coordinator", "coordinator")

timing = Timing(
    suspect_after_s=env_float("SUSPECT_AFTER_S", 4.0),
    dead_after_s=env_float("DEAD_AFTER_S", 6.0),
    forming_grace_s=env_float("FORMING_GRACE_S", 5.0),
)
num_shards = env_int("NUM_SHARDS", NUM_SHARDS)
partitioner = Partitioner(default_partition_map(num_shards=num_shards))
state = ClusterState(num_shards, now(), timing)
router = Router(state, partitioner)

DASHBOARD_HTML = (Path(__file__).parent / "static" / "dashboard.html").read_text()


async def monitor_loop() -> None:
    """Failure detector + repair loop. Runs every 0.5 s."""
    while True:
        try:
            state.tick(now())
            await router.repair_once()
        except Exception:
            log.exception("monitor loop error")
        await asyncio.sleep(0.5)


ANTI_ENTROPY_INTERVAL_S = env_float("ANTI_ENTROPY_INTERVAL_S", 30.0)


async def anti_entropy_loop() -> None:
    """Background check that IN_SYNC replicas really match their primaries (Router.anti_entropy_once)."""
    while True:
        await asyncio.sleep(ANTI_ENTROPY_INTERVAL_S)
        if state.is_forming(now()):
            continue
        try:
            await router.anti_entropy_once()
        except Exception:
            log.exception("anti-entropy round failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(monitor_loop())
    anti_entropy = asyncio.create_task(anti_entropy_loop())
    log.info("coordinator started", extra={"fields": {"shards": num_shards, "timing": timing.__dict__}})
    yield
    task.cancel()
    anti_entropy.cancel()
    await router.close()
    await mgmt_http.aclose()


app = FastAPI(title="Coordinator", lifespan=lifespan)
setup_app(app)


class NodeReport(BaseModel):
    node_id: str
    shard_id: int = Field(ge=0)
    url: str
    key_counts: dict = {}
    version_sum: int = 0


class PutBody(BaseModel):
    value: dict
    expected_version: int | None = Field(default=None, ge=0)


# ------------------------------------------------------------------ membership
@app.post("/internal/nodes/register")
async def register(report: NodeReport):
    node = state.register(report.node_id, report.shard_id, report.url, now(),
                          report.key_counts, report.version_sum)
    return {"node_id": node.node_id, "role": node.role, "sync": node.sync}


@app.post("/internal/nodes/heartbeat")
async def heartbeat(report: NodeReport):
    if not state.heartbeat(report.node_id, now(), report.key_counts, report.version_sum):
        raise NotFound("Unknown node, please register", code="UNKNOWN_NODE")
    node = state.nodes[report.node_id]
    return {"role": node.role, "sync": node.sync}


# ------------------------------------------------------------------ data
@app.get("/internal/data/{key}")
async def get_data(key: str):
    return await router.get(key)


@app.put("/internal/data/{key}")
async def put_data(key: str, body: PutBody):
    version = await router.put(key, body.value, body.expected_version)
    return {"key": key, "version": version}


@app.delete("/internal/data/{key}")
async def delete_data(key: str, expected_version: int | None = None):
    version = await router.delete(key, expected_version)
    return {"key": key, "version": version, "deleted": True}


@app.get("/internal/query")
async def query(kind: str, type: str | None = None, status: str | None = None):
    return await router.query(kind, type, status)


# ------------------------------------------------------------------ observability
@app.get("/internal/cluster")
async def cluster():
    view = state.to_json(now())
    view["partition_map"] = {str(p): s for p, s in partitioner.partition_map.items()}
    return view


RABBITMQ_MGMT_URL = env_str("RABBITMQ_MGMT_URL", "http://rabbitmq:15672")
RABBITMQ_AUTH = (env_str("RABBITMQ_USER", "guest"), env_str("RABBITMQ_PASS", "guest"))
mgmt_http = httpx.AsyncClient(timeout=2.0, auth=RABBITMQ_AUTH)


@app.get("/internal/queues")
async def queues():
    """Queue depths for the dashboard, from RabbitMQ's management API. Purely observational:
    if RabbitMQ can't be reached we say so instead of failing (the cluster view must keep working)."""
    try:
        r = await mgmt_http.get(f"{RABBITMQ_MGMT_URL}/api/queues/%2F")
        r.raise_for_status()
    except httpx.HTTPError as exc:
        return {"available": False, "error": exc.__class__.__name__, "queues": []}

    def rate(q: dict, key: str) -> float:
        return round(q.get("message_stats", {}).get(key, {}).get("rate", 0.0), 1)

    def order(name: str):  # main queues first, then retry queues by delay (2s, 4s, ... not "16s" < "2s")
        if name.startswith("matching.retry."):
            return (1, int(name.removeprefix("matching.retry.").removesuffix("s")), name)
        return (0, 0, name)

    return {"available": True, "queues": sorted(
        ({"name": q["name"], "ready": q.get("messages_ready", 0), "in_flight": q.get("messages_unacknowledged", 0),
          "consumers": q.get("consumers", 0), "in_rate": rate(q, "publish_details"),
          "out_rate": rate(q, "deliver_get_details")} for q in r.json()),
        key=lambda q: order(q["name"]))}


@app.get("/internal/verify")
async def verify():
    """Are primary and replica copies byte-for-byte identical? (digest comparison per shard)"""
    return await router.verify()


@app.get("/internal/locate/{key}")
async def locate(key: str):
    """FR-22: where does this key live? (for the data-distribution demo)"""
    partition, shard_id = partitioner.locate(key)
    return {
        "key": key,
        "partition": partition,
        "shard": shard_id,
        "primary": state.primary.get(shard_id),
        "nodes": [{"node_id": n.node_id, "role": n.role, "health": n.health, "sync": n.sync}
                  for n in state.shard_nodes(shard_id)],
        "how": f"sha1('{key}') mod 12 = {partition_of(key)} -> partition map -> shard {shard_id}",
    }


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return DASHBOARD_HTML


@app.get("/ready", include_in_schema=False)
async def ready():
    return {"status": "ok", "forming": state.is_forming(now())}

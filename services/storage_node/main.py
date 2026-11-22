"""Storage node: one member of one shard (DESIGN.md §5.2).

A storage node is deliberately "dumb". It does not know whether it is primary or replica;
the coordinator decides that and sends the right kind of write. Because nodes never promote
themselves, two nodes can never both believe they are primary (no split brain, ADR-05).

What it does:
  * registers with the coordinator, then sends a heartbeat every HEARTBEAT_INTERVAL_S;
  * serves the internal data API (spec Table 3) on its local SQLite store;
  * on request, resyncs itself from another node (POST /internal/sync, DESIGN.md §5.5).
"""

import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Query
from pydantic import BaseModel

from services.common.config import COORDINATOR_URL, env_float, env_int, env_str
from services.common.errors import Conflict, NotFound, VersionConflict
from services.common.http import make_client, setup_app
from services.common.logging import setup_logging
from services.storage_node.store import KVStore, VersionMismatch

NODE_ID = env_str("NODE_ID", "shard-0-0")
SHARD_ID = env_int("SHARD_ID", 0)
NODE_URL = env_str("NODE_URL", "http://localhost:8000")
DATA_DIR = env_str("DATA_DIR", "./data")
HEARTBEAT_INTERVAL_S = env_float("HEARTBEAT_INTERVAL_S", 2.0)

log = setup_logging("storage-node", NODE_ID)
store = KVStore(f"{DATA_DIR}/{NODE_ID}.db")


async def heartbeat_loop() -> None:
    """Heartbeat pattern (spec §8). If the coordinator answers 404 to a heartbeat, it does not
    know us (it restarted and lost its soft state, ADR-14), so we register again."""
    registered = False
    async with make_client(COORDINATOR_URL, timeout=2.0) as http:
        while True:
            stats = store.stats()
            body = {
                "node_id": NODE_ID,
                "shard_id": SHARD_ID,
                "url": NODE_URL,
                "key_counts": stats["counts"],
                "version_sum": stats["version_sum"],
            }
            try:
                if not registered:
                    r = await http.post("/internal/nodes/register", json=body)
                    r.raise_for_status()
                    registered = True
                    log.info("registered with coordinator", extra={"fields": r.json()})
                else:
                    r = await http.post("/internal/nodes/heartbeat", json=body)
                    if r.status_code == 404:
                        log.warning("coordinator does not know us any more; re-registering")
                        registered = False
                        continue
                    r.raise_for_status()
            except httpx.HTTPError as exc:
                log.warning(f"coordinator unreachable ({exc.__class__.__name__}); will retry")
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(heartbeat_loop())
    log.info("storage node started", extra={"fields": {"shard": SHARD_ID, "url": NODE_URL, **store.stats()}})
    yield
    task.cancel()
    store.close()


app = FastAPI(title=f"Storage node {NODE_ID}", lifespan=lifespan)
setup_app(app)


class PutBody(BaseModel):
    value: dict
    expected_version: int | None = None


class ReplicateBody(BaseModel):
    key: str
    value: dict | None
    version: int
    deleted: bool = False


class SyncBody(BaseModel):
    source_url: str


# Plain `def` endpoints: FastAPI runs them in a thread pool, so blocking SQLite calls do not
# stall the event loop (and the heartbeat task keeps running).


@app.get("/internal/data/{key}")
def get_data(key: str, include_deleted: bool = False):
    record = store.get(key, include_deleted=include_deleted)
    if record is None:
        raise NotFound(f"Key {key} not found")
    return record.to_json()


@app.put("/internal/data/{key}")
def put_data(key: str, body: PutBody):
    """Primary write: compare-and-set on the version (ADR-06)."""
    try:
        version = store.put_primary(key, body.value, body.expected_version)
    except VersionMismatch as exc:
        raise VersionConflict(
            f"Expected version {body.expected_version}, found {exc.current_version}",
            details={"current_version": exc.current_version, "exists": exc.exists},
        )
    return {"key": key, "version": version}


@app.delete("/internal/data/{key}")
def delete_data(key: str, expected_version: int | None = None):
    try:
        version = store.delete_primary(key, expected_version)
    except VersionMismatch as exc:
        raise VersionConflict(f"Expected version {expected_version}, found {exc.current_version}")
    if version is None:
        raise NotFound(f"Key {key} not found")
    return {"key": key, "version": version, "deleted": True}


@app.post("/internal/replicate")
def replicate(body: ReplicateBody):
    """Replica write forwarded by the coordinator, carrying the primary's version."""
    applied = store.apply_replica(body.key, body.value, body.version, body.deleted)
    return {"applied": applied}


@app.get("/internal/query")
def query(kind: str, type: str | None = None, status: str | None = None):
    return {"items": [r.to_json() for r in store.query(kind, type, status)]}


@app.get("/internal/digest")
def digest():
    return {"node_id": NODE_ID, "digest": store.digest()}


@app.get("/internal/stats")
def stats():
    return {"node_id": NODE_ID, "shard_id": SHARD_ID, **store.stats()}


@app.get("/internal/dump", include_in_schema=False)
def dump(limit: int = Query(50, le=1000)):
    """Debug helper for the demo: what does this node physically hold?"""
    with store._lock:
        rows = store._db.execute(
            "SELECT key, version, deleted FROM kv ORDER BY updated_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return {"node_id": NODE_ID, "keys": [{"key": k, "version": v, "deleted": bool(d)} for k, v, d in rows]}


@app.post("/internal/sync")
async def sync(body: SyncBody):
    """Catch-up recovery (DESIGN.md §5.5). Make this node identical to `source_url`
    (the current primary), copying only the keys that differ: a delta, not a full copy.

      1. fetch the primary's digest and compare it with ours;
      2. for every key whose (version, hash) differs: fetch it and overwrite ours ("primary wins");
      3. delete keys the primary does not have at all (writes that were never acknowledged).

    The coordinator keeps sending live writes to us meanwhile, and re-checks the digests
    afterwards; if a write raced with the sync it simply runs the sync again.
    """
    if body.source_url.rstrip("/") == NODE_URL.rstrip("/"):
        raise Conflict("Cannot sync from myself")
    pulled = removed = 0
    async with make_client(body.source_url, timeout=10.0) as http:
        r = await http.get("/internal/digest")
        r.raise_for_status()
        source = r.json()["digest"]
        local = await asyncio.to_thread(store.digest)

        for key, (s_version, s_hash, _s_deleted) in source.items():
            mine = local.get(key)
            if mine and mine[0] == s_version and mine[1] == s_hash:
                continue
            r = await http.get(f"/internal/data/{key}", params={"include_deleted": "true"})
            if r.status_code == 404:  # deleted for real on the source in the meantime
                continue
            r.raise_for_status()
            rec = r.json()
            await asyncio.to_thread(store.force_apply, key, rec["value"], rec["version"], rec["deleted"])
            pulled += 1

        for key in local.keys() - source.keys():
            await asyncio.to_thread(store.hard_delete, key)
            removed += 1

    log.info("resync finished", extra={"fields": {"source": body.source_url, "pulled": pulled, "removed": removed}})
    return {"node_id": NODE_ID, "pulled": pulled, "removed": removed, "source_keys": len(source)}

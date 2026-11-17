"""End-to-end tests against a running stack (docker compose or minikube port-forward).

    docker compose up -d --build
    uv run pytest tests/integration

GATEWAY_URL (default http://localhost:8080) is the public API; COORDINATOR_URL
(default http://localhost:8090) is used only to inspect data placement.

Every test uses its own random location far from the others, so tests don't steal each
other's resources (matching radius is 50 km).
"""

import asyncio
import os
import random
import time

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:8080")
COORDINATOR = os.environ.get("COORDINATOR_URL", "http://localhost:8090")


def isolated_spot() -> dict:
    # A random point; two spots are almost surely > 50 km apart.
    return {"lat": round(random.uniform(-60, 60), 4), "lon": round(random.uniform(-170, 170), 4)}


def near(spot: dict, km: float) -> dict:
    return {"lat": spot["lat"] + km / 111.0, "lon": spot["lon"]}  # 1 degree of latitude ~ 111 km


@pytest.fixture
async def api():
    async with httpx.AsyncClient(base_url=GATEWAY, timeout=15) as c:
        try:
            await c.get("/resources", params={"type": "water"})
        except httpx.TransportError:
            pytest.skip(f"stack not running at {GATEWAY}")
        yield c


async def register(api, type_, location, quantity=1, name="r"):
    r = await api.post("/resources", json={"type": type_, "name": name, "quantity": quantity, "location": location})
    assert r.status_code == 201, r.text
    return r.json()


async def create(api, type_, location, quantity=1, urgency=3):
    r = await api.post("/requests", json={"type": type_, "location": location, "quantity": quantity,
                                          "urgency": urgency, "details": "test"})
    assert r.status_code == 201, r.text
    return r.json()


async def wait_status(api, request_id, statuses, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await api.get(f"/requests/{request_id}")).json()
        if body["status"] in statuses:
            return body
        await asyncio.sleep(0.2)
    raise AssertionError(f"request {request_id} stuck in {body['status']}")


# ---------------------------------------------------------------- requests
async def test_create_get_and_lifecycle(api):
    spot = isolated_spot()
    created = await create(api, "medicine", spot)
    assert created["status"] == "PENDING" and created["version"] == 1

    got = await api.get(f"/requests/{created['id']}")
    assert got.status_code == 200 and got.json()["id"] == created["id"]
    assert got.headers["etag"] == f'"{got.json()["version"]}"'

    # nothing nearby -> UNMATCHED; then the client can cancel it
    await wait_status(api, created["id"], {"UNMATCHED"})
    r = await api.put(f"/requests/{created['id']}/status", json={"status": "CANCELLED"})
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
    statuses = [h["status"] for h in r.json()["history"]]
    assert statuses == ["PENDING", "UNMATCHED", "CANCELLED"]


async def test_error_codes(api):
    assert (await api.post("/requests", json={"type": "pizza"})).status_code == 400
    assert (await api.get("/requests/does-not-exist")).status_code == 404
    req = await create(api, "food", isolated_spot())
    # ASSIGNED is set only by matching
    r = await api.put(f"/requests/{req['id']}/status", json={"status": "ASSIGNED"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "INVALID_TRANSITION"
    # stale If-Match -> 409 VERSION_CONFLICT
    r = await api.put(f"/requests/{req['id']}/status", json={"status": "CANCELLED"}, headers={"If-Match": '"999"'})
    assert r.status_code == 409 and r.json()["error"]["code"] == "VERSION_CONFLICT"
    assert (await api.post("/match/does-not-exist")).status_code == 404


# ---------------------------------------------------------------- resources
async def test_register_list_and_status(api):
    spot = isolated_spot()
    res = await register(api, "shelter", spot, quantity=3, name="School hall")
    assert res["status"] == "AVAILABLE" and res["availability"]["quantity_remaining"] == 3

    listed = (await api.get("/resources", params={"type": "shelter", "status": "AVAILABLE"})).json()
    assert any(r["id"] == res["id"] for r in listed["items"]) and listed["partial"] is False

    r = await api.put(f"/resources/{res['id']}/status", json={"status": "OFFLINE"})
    assert r.status_code == 200 and r.json()["status"] == "OFFLINE"
    r = await api.put(f"/resources/{res['id']}/status", json={"status": "AVAILABLE", "quantity_remaining": 0})
    assert r.json()["status"] == "EXHAUSTED"


# ---------------------------------------------------------------- matching
async def test_async_match_picks_nearest_with_enough_quantity(api):
    spot = isolated_spot()
    far = await register(api, "water", near(spot, 20), quantity=10, name="far tanker")
    small = await register(api, "water", near(spot, 1), quantity=2, name="close but small")
    close = await register(api, "water", near(spot, 5), quantity=10, name="close tanker")
    req = await create(api, "water", spot, quantity=4, urgency=5)

    done = await wait_status(api, req["id"], {"ASSIGNED", "UNMATCHED"})
    assert done["status"] == "ASSIGNED"
    assert done["assignment"]["resource_id"] == close["id"]  # small one has too little, far one is further
    assert 4.5 < done["assignment"]["distance_km"] < 5.5
    res = (await api.get(f"/resources/{close['id']}")).json()
    assert res["availability"]["quantity_remaining"] == 6
    assert (await api.get(f"/resources/{far['id']}")).json()["availability"]["quantity_remaining"] == 10
    assert (await api.get(f"/resources/{small['id']}")).json()["availability"]["quantity_remaining"] == 2


async def test_sync_match_endpoint_and_cancel_compensation(api):
    spot = isolated_spot()
    req = await create(api, "transportation", spot)
    await wait_status(api, req["id"], {"UNMATCHED"})  # nothing there yet

    r = await api.post(f"/match/{req['id']}")
    assert r.status_code == 409 and r.json()["error"]["code"] == "NO_RESOURCE_AVAILABLE"

    bus = await register(api, "transportation", near(spot, 2), quantity=1, name="bus")
    # Two paths race for this request now: our POST /match and the automatic wake-up triggered
    # by registering the bus. Exactly one wins; the other sees NOT_MATCHABLE. Either way the
    # request must end up on the bus, and the bus must be used exactly once.
    r = await api.post(f"/match/{req['id']}")
    assert (r.status_code == 200 and r.json()["resource_id"] == bus["id"]) or \
           (r.status_code == 409 and r.json()["error"]["code"] == "NOT_MATCHABLE"), r.text
    done = await wait_status(api, req["id"], {"ASSIGNED"})
    assert done["assignment"]["resource_id"] == bus["id"]
    assert (await api.get(f"/resources/{bus['id']}")).json()["status"] == "EXHAUSTED"

    # cancel -> request.cancelled event -> resource-service releases the quantity (saga compensation)
    await api.put(f"/requests/{req['id']}/status", json={"status": "CANCELLED"})
    for _ in range(50):
        res = (await api.get(f"/resources/{bus['id']}")).json()
        if res["availability"]["quantity_remaining"] == 1:
            break
        await asyncio.sleep(0.2)
    assert res["status"] == "AVAILABLE" and res["allocations"] == []


async def test_wakeup_on_new_and_freed_supply(api):
    """FR-19: waiting requests are matched automatically when supply appears or is freed,
    without anyone calling POST /match."""
    spot = isolated_spot()
    first = await create(api, "shelter", spot, quantity=10, urgency=5)
    second = await create(api, "shelter", near(spot, 1), quantity=10, urgency=4)
    await wait_status(api, first["id"], {"UNMATCHED"})
    await wait_status(api, second["id"], {"UNMATCHED"})

    hall = await register(api, "shelter", near(spot, 3), quantity=10, name="hall")  # room for only one
    # Both are woken up; urgency orders the queue, but two matching pods may run them at the same
    # moment, so we only require that exactly one gets the hall.
    deadline = asyncio.get_running_loop().time() + 15
    while asyncio.get_running_loop().time() < deadline:
        states = {r["id"]: (await api.get(f"/requests/{r['id']}")).json() for r in (first, second)}
        winners = [s for s in states.values() if s["status"] == "ASSIGNED"]
        if winners:
            break
        await asyncio.sleep(0.3)
    assert len(winners) == 1 and winners[0]["assignment"]["resource_id"] == hall["id"]
    loser = next(s for s in states.values() if s["id"] != winners[0]["id"])
    assert loser["status"] == "UNMATCHED"

    # Cancelling the winner frees the hall -> resource.released -> the other one is woken up.
    await api.put(f"/requests/{winners[0]['id']}/status", json={"status": "CANCELLED"})
    done = await wait_status(api, loser["id"], {"ASSIGNED"}, timeout=15)
    assert done["assignment"]["resource_id"] == hall["id"]


async def test_no_over_allocation_under_concurrency(api):
    """FR-18: 50 simultaneous requests for 1 unit each vs a single resource holding 10 units.
    Exactly 10 must be assigned, 40 unmatched, and the resource must end at 0 (never negative)."""
    spot = isolated_spot()
    res = await register(api, "food", near(spot, 3), quantity=10, name="food depot")
    reqs = await asyncio.gather(*(create(api, "food", spot) for _ in range(50)))

    finals = await asyncio.gather(*(wait_status(api, r["id"], {"ASSIGNED", "UNMATCHED"}, timeout=60) for r in reqs))
    assigned = [f for f in finals if f["status"] == "ASSIGNED"]
    assert len(assigned) == 10
    assert all(f["assignment"]["resource_id"] == res["id"] for f in assigned)

    final_res = (await api.get(f"/resources/{res['id']}")).json()
    assert final_res["availability"]["quantity_remaining"] == 0
    assert final_res["status"] == "EXHAUSTED"
    assert sorted(a["request_id"] for a in final_res["allocations"]) == sorted(f["id"] for f in assigned)


# ---------------------------------------------------------------- distribution
async def test_data_is_spread_over_three_shards(api):
    reqs = await asyncio.gather(*(create(api, "shelter", isolated_spot()) for _ in range(30)))
    async with httpx.AsyncClient(base_url=COORDINATOR, timeout=5) as coord:
        shards = [(await coord.get(f"/internal/locate/req:{r['id']}")).json()["shard"] for r in reqs]
    counts = {s: shards.count(s) for s in range(3)}
    assert all(c > 0 for c in counts.values()), counts  # every shard got some of the 30 keys

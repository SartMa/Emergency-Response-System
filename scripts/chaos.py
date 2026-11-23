"""Chaos test: real traffic + random storage-node kills, then automatic checks of our guarantees.

    uv run python scripts/chaos.py                    # Kubernetes (needs k8s-gateway + k8s-dashboard)
    uv run python scripts/chaos.py --mode compose     # docker compose
    uv run python scripts/chaos.py --duration 180 --kill-every 6 --report chaos-report.json

What it does
  * Registers a few food depots at an isolated spot, with a known total quantity.
  * Runs N client workers that keep creating requests near the depots and cancelling some of
    them (which exercises the saga's compensation path while nodes are dying).
  * Meanwhile the "chaos monkey" repeatedly hard-kills a random storage node (primary or
    replica), waiting for the cluster to heal in between, since we claim to survive one failure
    per shard at a time.
  * Then it stops the traffic, waits for everything to settle, and checks:

    1. Durability        every request the client got 201 for still exists
    2. Durable updates   every cancellation the client got 200 for is still CANCELLED
    3. No over-allocation  each depot: 0 <= remaining, and total - remaining = sum of allocations
    4. Cross-service consistency  depot allocations == requests assigned to those depots
    5. Liveness          no queued request is stuck in PENDING
    6. Replica convergence  every replica is byte-for-byte identical to its primary (digests)

A run passes only if all six hold. Failed client calls (503/504) are allowed: they were never
acknowledged, so the system made no promise about them. They are reported as availability.
"""

import argparse
import asyncio
import json
import random
import subprocess
import sys
import time
from collections import Counter

import httpx

TERMINAL_OR_SETTLED = {"ASSIGNED", "UNMATCHED", "CANCELLED", "IN_PROGRESS", "FULFILLED"}
HOLDS_ALLOCATION = {"ASSIGNED", "IN_PROGRESS", "FULFILLED"}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Chaos:
    def __init__(self, args):
        self.args = args
        self.api = httpx.AsyncClient(base_url=args.gateway, timeout=20)
        self.coord = httpx.AsyncClient(base_url=args.coord, timeout=20)
        self.stop = asyncio.Event()
        self.acked: dict[str, dict] = {}  # request id -> {"queued": bool, "cancel_acked": bool}
        self.codes: Counter = Counter()
        self.kills: list[dict] = []
        self.events: dict[tuple, dict] = {}
        self.latencies: list[float] = []

    # ------------------------------------------------------------------ helpers
    async def cluster(self) -> dict | None:
        try:
            r = await self.coord.get("/internal/cluster")
            return r.json() if r.status_code == 200 else None
        except httpx.HTTPError:
            return None

    async def healthy(self) -> bool:
        c = await self.cluster()
        if not c or c["forming"]:
            return False
        nodes = [n for s in c["shards"] for n in s["nodes"]]
        return (len(nodes) == 6 and all(s["available"] for s in c["shards"])
                and all(n["health"] == "ALIVE" and n["sync"] == "IN_SYNC" for n in nodes))

    async def wait_healthy(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await self.healthy():
                return True
            await asyncio.sleep(1)
        return False

    def kill(self, node: str) -> None:
        if self.args.mode == "k8s":
            # --force --grace-period=0: an abrupt crash, not a polite shutdown
            cmd = ["kubectl", "-n", "erm", "delete", "pod", node, "--grace-period=0", "--force", "--wait=false"]
        else:
            cmd = ["docker", "compose", "kill", node]
        subprocess.run(cmd, capture_output=True)

    def revive(self, node: str) -> None:
        if self.args.mode == "compose":  # Kubernetes recreates the pod by itself (StatefulSet)
            subprocess.run(["docker", "compose", "start", node], capture_output=True)

    # ------------------------------------------------------------------ setup
    async def setup(self) -> None:
        # An isolated spot (middle of the Indian Ocean) so no other data competes for these depots.
        self.spot = (round(random.uniform(-30, -10), 3), round(random.uniform(70, 90), 3))
        self.depots: dict[str, int] = {}
        for i in range(self.args.depots):
            body = {"type": "food", "name": f"chaos depot {i}", "quantity": self.args.depot_quantity,
                    "location": {"lat": self.spot[0] + i * 0.05, "lon": self.spot[1], "label": f"chaos-{i}"}}
            r = await self.api.post("/resources", json=body)
            r.raise_for_status()
            self.depots[r.json()["id"]] = self.args.depot_quantity
        log(f"registered {len(self.depots)} depots x {self.args.depot_quantity} units at {self.spot}")

    # ------------------------------------------------------------------ load
    async def worker(self, wid: int) -> None:
        rng = random.Random(wid)
        while not self.stop.is_set():
            if self.acked and rng.random() < self.args.cancel_ratio:
                rid = rng.choice(list(self.acked))
                if not self.acked[rid]["cancel_acked"]:
                    try:
                        r = await self.api.put(f"/requests/{rid}/status", json={"status": "CANCELLED"})
                        self.codes[f"cancel {r.status_code}"] += 1
                        if r.status_code == 200:
                            self.acked[rid]["cancel_acked"] = True
                    except httpx.HTTPError as exc:
                        self.codes[f"cancel {exc.__class__.__name__}"] += 1
            else:
                body = {"type": "food", "quantity": rng.randint(1, 3), "urgency": rng.randint(1, 5),
                        "location": {"lat": self.spot[0] + rng.uniform(0, 0.1), "lon": self.spot[1] + rng.uniform(-0.05, 0.05)}}
                t0 = time.monotonic()
                try:
                    r = await self.api.post("/requests", json=body)
                    self.codes[f"create {r.status_code}"] += 1
                    if r.status_code == 201:
                        self.latencies.append(time.monotonic() - t0)
                        b = r.json()
                        self.acked[b["id"]] = {"queued": b["queued_for_matching"], "cancel_acked": False}
                except httpx.HTTPError as exc:
                    self.codes[f"create {exc.__class__.__name__}"] += 1
            await asyncio.sleep(rng.uniform(0.02, 0.15))

    # ------------------------------------------------------------------ chaos monkey
    async def monkey(self) -> None:
        await asyncio.sleep(3)
        while not self.stop.is_set():
            if not self.args.aggressive and not await self.wait_healthy(90):
                log("cluster did not heal within 90 s; continuing anyway")
            if self.stop.is_set():
                break
            c = await self.cluster()
            if c:
                nodes = [n for s in c["shards"] for n in s["nodes"] if n["health"] != "DEAD"]
                victim = random.choice(nodes)
                log(f"KILL {victim['node_id']} ({victim['role']})")
                self.kill(victim["node_id"])
                self.kills.append({"t": time.time(), "node": victim["node_id"], "role": victim["role"]})
                if self.args.mode == "compose":
                    await asyncio.sleep(random.uniform(2, 8))  # stay down for a while
                    self.revive(victim["node_id"])
            await asyncio.sleep(self.args.kill_every * random.uniform(0.5, 1.5))

    async def event_recorder(self) -> None:
        while not self.stop.is_set():
            c = await self.cluster()
            if c:
                for e in c["events"]:
                    self.events.setdefault((round(e["t"], 2), e["message"]), e)
            await asyncio.sleep(1)

    # ------------------------------------------------------------------ checks
    async def fetch_state(self) -> tuple[dict, dict]:
        reqs = {r["id"]: r for r in (await self.api.get("/requests", params={"type": "food"})).json()["items"]}
        missing = [rid for rid in self.acked if rid not in reqs]
        for rid in missing:  # double-check individually: a list can be partial
            r = await self.api.get(f"/requests/{rid}")
            if r.status_code == 200:
                reqs[rid] = r.json()
        depots = {}
        for did in self.depots:
            r = await self.api.get(f"/resources/{did}")
            depots[did] = r.json() if r.status_code == 200 else None
        return reqs, depots

    def evaluate(self, reqs: dict, depots: dict, verify: dict) -> list[tuple[str, bool, str]]:
        ours = {rid: reqs.get(rid) for rid in self.acked}
        lost = [rid for rid, r in ours.items() if r is None]
        bad_cancel = [rid for rid, a in self.acked.items()
                      if a["cancel_acked"] and ours[rid] and ours[rid]["status"] != "CANCELLED"]

        over = []
        alloc_pairs = set()
        for did, d in depots.items():
            if d is None:
                over.append(f"{did[:8]} missing")
                continue
            av = d["availability"]
            allocated = sum(a["quantity"] for a in d["allocations"])
            if av["quantity_remaining"] < 0 or av["quantity_total"] - av["quantity_remaining"] != allocated:
                over.append(f"{d['name']}: total {av['quantity_total']}, remaining {av['quantity_remaining']}, allocated {allocated}")
            alloc_pairs |= {(a["request_id"], did) for a in d["allocations"]}

        assigned_pairs = {(rid, r["assignment"]["resource_id"]) for rid, r in ours.items()
                          if r and r["status"] in HOLDS_ALLOCATION and r.get("assignment")
                          and r["assignment"]["resource_id"] in self.depots}
        orphan_alloc = alloc_pairs - assigned_pairs   # quantity held for a request that doesn't have it
        missing_alloc = assigned_pairs - alloc_pairs  # request thinks it has quantity the depot doesn't record
        stuck = [rid for rid, r in ours.items() if r and r["status"] == "PENDING" and self.acked[rid]["queued"]]

        # IDs behind each failed check (first 20), written to the JSON report for debugging
        self.failures = {"lost": lost[:20], "reverted_cancels": bad_cancel[:20],
                         "orphaned_allocations": sorted(orphan_alloc)[:20],
                         "missing_allocations": sorted(missing_alloc)[:20], "stuck_pending": stuck[:20]}
        diverged = [f"shard {s['shard_id']}: " + ", ".join(f"{n['node_id']} differs in {n.get('differing_keys')} keys"
                                                          for n in s["nodes"] if not n["identical"])
                    for s in verify["shards"] if not s["consistent"]]
        return [
            ("1 durability: acknowledged requests exist", not lost, f"{len(self.acked)} acknowledged, {len(lost)} lost"),
            ("2 acknowledged cancellations persisted", not bad_cancel,
             f"{sum(a['cancel_acked'] for a in self.acked.values())} cancelled, {len(bad_cancel)} reverted"),
            ("3 no over-allocation", not over, "; ".join(over) or f"{len(self.depots)} depots balanced"),
            ("4 depots and requests agree", not orphan_alloc and not missing_alloc,
             f"{len(alloc_pairs)} allocations; {len(orphan_alloc)} orphaned, {len(missing_alloc)} missing"),
            ("5 no queued request stuck PENDING", not stuck, f"{len(stuck)} stuck"),
            ("6 replicas identical to primaries", verify["consistent"], "; ".join(diverged) or
             f"all 3 shards identical ({sum(s['keys'] or 0 for s in verify['shards'])} keys)"),
        ]

    async def settle_and_check(self) -> list[tuple[str, bool, str]]:
        log("load stopped; waiting for the cluster to heal and the queue to drain...")
        await self.wait_healthy(120)
        results = []
        for attempt in range(30):  # cancel events and resyncs are asynchronous: give them time
            reqs, depots = await self.fetch_state()
            verify = (await self.coord.get("/internal/verify")).json()
            results = self.evaluate(reqs, depots, verify)
            if all(ok for _, ok, _ in results):
                break
            await asyncio.sleep(2)
        return results

    # ------------------------------------------------------------------ run
    async def run(self) -> int:
        if not await self.wait_healthy(30):
            log("cluster is not healthy before the test; fix that first (see README.md)")
            return 2
        await self.setup()
        started = time.time()
        tasks = [asyncio.create_task(self.worker(i)) for i in range(self.args.workers)]
        tasks += [asyncio.create_task(self.monkey()), asyncio.create_task(self.event_recorder())]
        log(f"running {self.args.workers} client workers + chaos monkey for {self.args.duration}s")
        await asyncio.sleep(self.args.duration)
        self.stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        results = await self.settle_and_check()

        creates = {k: v for k, v in self.codes.items() if k.startswith("create")}
        ok = creates.get("create 201", 0)
        total = sum(creates.values())
        kinds = Counter(e["kind"] for e in self.events.values())
        lat = sorted(self.latencies)
        pct = lambda p: round(lat[int(len(lat) * p) - 1] * 1000) if lat else None

        print("\n" + "=" * 72)
        print(f" CHAOS TEST  {self.args.mode}  {round(time.time() - started)}s  "
              f"{len(self.kills)} node kills ({sum(k['role'] == 'PRIMARY' for k in self.kills)} primaries)")
        print("=" * 72)
        for name, passed, detail in results:
            print(f" {'PASS' if passed else 'FAIL'}  {name:<42} {detail}")
        print("-" * 72)
        print(f" writes accepted: {ok}/{total} ({100 * ok / max(total, 1):.1f}%)   "
              f"latency p50 {pct(.5)} ms, p95 {pct(.95)} ms, max {pct(1.0)} ms")
        print(f" client outcomes: {dict(self.codes)}")
        print(f" cluster events:  {kinds.get('failover', 0)} failovers, {kinds.get('primary', 0)} promotions/elections, "
              f"{kinds.get('in_sync', 0)} resyncs, {kinds.get('dead', 0)} nodes declared dead")
        resynced = [e["message"] for e in self.events.values() if e["kind"] == "in_sync"]
        if resynced:
            print(f" e.g.             {resynced[-1]}")
        print("=" * 72)
        passed = all(p for _, p, _ in results)
        print(" RESULT:", "ALL GUARANTEES HELD" if passed else "GUARANTEE VIOLATED")

        if self.args.report:
            with open(self.args.report, "w") as f:
                json.dump({"mode": self.args.mode, "duration_s": self.args.duration, "kills": self.kills,
                           "checks": [{"check": n, "passed": p, "detail": d} for n, p, d in results],
                           "failures": getattr(self, "failures", {}),
                           "client_outcomes": dict(self.codes), "events": sorted(self.events.values(), key=lambda e: e["t"]),
                           "latency_ms": {"p50": pct(.5), "p95": pct(.95), "max": pct(1.0)}}, f, indent=2)
            print(f" report written to {self.args.report}")
        await self.api.aclose()
        await self.coord.aclose()
        return 0 if passed else 1


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--mode", choices=["k8s", "compose"], default="k8s")
    p.add_argument("--gateway", default="http://localhost:8080")
    p.add_argument("--coord", default="http://localhost:8090")
    p.add_argument("--duration", type=int, default=120, help="seconds of traffic + chaos")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--kill-every", type=float, default=8, help="average seconds between kills (after healing)")
    p.add_argument("--aggressive", action="store_true", help="don't wait for the cluster to heal between kills")
    p.add_argument("--depots", type=int, default=4)
    p.add_argument("--depot-quantity", type=int, default=60)
    p.add_argument("--cancel-ratio", type=float, default=0.15)
    p.add_argument("--report", help="write a JSON report here")
    sys.exit(asyncio.run(Chaos(p.parse_args()).run()))


if __name__ == "__main__":
    main()

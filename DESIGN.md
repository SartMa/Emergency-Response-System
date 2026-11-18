# Distributed Emergency Resource Matching System — Design

> **Status: FROZEN (2026-11-16).** From now on, any change needs an ADR update (§7) and team approval.
>
> Spec: `Emergency Response Matching-1.pdf` · Deadline: **2026-11-20**

---

## 1. Goals in one paragraph

People affected by a disaster create emergency requests (water, food, medicine, transportation, shelter). Volunteers, NGOs and supply holders register resources. The system matches each request **asynchronously** to the **nearest suitable available** resource, and it **never assigns the same unit of capacity twice**. Data is spread across **3 shards × 2 nodes** (primary + replica) behind a **coordinator**. The system keeps working when a storage node dies, and repairs that node when it comes back. Everything runs in Docker and Kubernetes.

---

## 2. Functional requirements

### 2.1 From the spec (mandatory)

| ID | Requirement | Spec ref |
|---|---|---|
| FR-1 | `createRequest(type, location, details)`: store the request, return 201 with an ID, and start asynchronous matching | §1, §7 |
| FR-2 | `getRequest(id)`: return the request, including status and assigned resource | §1 |
| FR-3 | `updateStatus(requestId, status)`: change the request status, only through valid transitions (§4.3) | §1 |
| FR-4 | `registerResource(type, location, availability)`: store the resource, return 201 | §1 |
| FR-5 | `GET /resources`: list resources, with optional `type` / `status` filters | §5 |
| FR-6 | `PUT /resources/{id}/status`: change resource status/availability | §5 |
| FR-7 | `matchRequest(requestId)`: synchronous match on demand (`POST /match/{id}`) | §1, §5 |
| FR-8 | Matching considers at least **type** and **location** | §7 |
| FR-9 | Request → RabbitMQ → Matching Service pipeline (asynchronous) | §6, §7 |
| FR-10 | Coordinator routes every storage operation by hash; clients never see shard locations | §4 |
| FR-11 | Storage nodes register with the coordinator and send heartbeats | §6, §8 |
| FR-12 | Detect dead nodes, stop routing to them, serve from the replica, keep processing | §8 |
| FR-13 | When a node recovers, it is synchronised before it serves reads again | §8 |
| FR-14 | JSON everywhere; correct HTTP codes (2xx / 400 / 404 / 409 / 503) | §5 |

### 2.2 Our additions (the features that set this project apart)

**Tier 1** = build now, before handover. **Tier 2** = add later, in the cut order of §8.4.

| ID | Requirement | Tier | Why it matters |
|---|---|---|---|
| FR-15 | **Geo matching**: locations are lat/lon; ranked by great-circle (haversine) distance within a max radius (`nearest` strategy; `balanced` is Tier 2) | 1 | "Location" means real distance, not string equality |
| FR-16 | **Urgency 1–5** on requests → RabbitMQ **priority queue** | 1 | Medicine for a critical patient goes before a shelter query |
| FR-17 | **Resource quantity**: a resource has a quantity (e.g. 40 food packets); a request asks for a quantity (default 1) and is matched only to a resource with enough left (no splitting across resources). A specific volunteer or vehicle is registered with quantity 1 | 1 | Real-world model; shows concurrency control |
| FR-18 | **No over-allocation, even under concurrency**, through optimistic concurrency (versions + `If-Match`) | 1 | Correctness under scaling; other teams will have a race here |
| FR-19 | **Auto-retry** of unmatched requests with TTL backoff + DLQ; a new resource of that type **wakes up** waiting `UNMATCHED` requests | 2 | Supply often arrives after demand. Tier 1 behaviour: no match → `UNMATCHED`; `POST /match` retries manually |
| FR-20 | **Cancel → compensation**: cancelling an assigned request returns its quantity to the resource (saga) | 1 | Keeps request and resource consistent without distributed transactions |
| FR-21 | **Live cluster dashboard**: partition map, node roles and health, key counts (queue depths in Tier 2), updated every second | 1 | Makes the failure/recovery demo visible |
| FR-22 | `GET /internal/locate/{key}`: shows partition → shard → nodes for any key | 1 | Makes the "data distribution" demo trivial |
| FR-23 | **Status history** (audit trail) stored on each request | 1 | Traceability; easy to show in the demo |

Also Tier 2: reconciler for lost messages (§6.2), `balanced` strategy (§6.3), HPA, Prometheus `/metrics`.

---

## 3. Non-functional requirements

| ID | Quality | Target / mechanism |
|---|---|---|
| NFR-1 | **Availability** | Survive the loss of **any one node per shard** with no failed client operations (fast failover on error, §5.4) |
| NFR-2 | **Durability** | An acknowledged write exists on primary **and** replica (synchronous replication). Each node persists to SQLite on a PersistentVolume |
| NFR-3 | **Consistency** | Reads go to the shard primary → read-your-writes. Conditional writes → no lost updates and no double assignment |
| NFR-4 | **Fault tolerance** | Failure detection within ≤ 6 s by heartbeat, immediately on a request error. Recovered node is resynced automatically |
| NFR-5 | **Scalability** | Request, resource and matching services are stateless and scale horizontally; matching pods are competing consumers |
| NFR-6 | **Performance** (local minikube) | Create request p95 < 200 ms; asynchronous match completes < 2 s after create when supply exists |
| NFR-7 | **Observability** | Structured JSON logs with a **correlation ID** that follows a request across HTTP and AMQP; dashboard; RabbitMQ management UI |
| NFR-8 | **Maintainability** | Hexagonal layering per service; pluggable strategies; one shared `common` package; ADRs for every decision |
| NFR-9 | **Testability** | Domain logic is pure (no I/O) and unit-tested. Integration tests run on docker-compose. Demo scripts mirror spec §10 |
| NFR-10 | **Portability** | Same images run on docker-compose and minikube; all config through env vars / ConfigMap |
| NFR-11 | **Graceful degradation** | If RabbitMQ is down, requests are still stored (201) and queued later by the reconciler. If a shard is fully down, list queries return `partial: true` instead of failing |
| NFR-12 | **Security** (minimal, stated) | Only the Ingress is exposed; `/internal/*` is reachable only inside the cluster; RabbitMQ creds in a Secret. Auth is out of scope |

---

## 4. Architecture

### 4.1 Style
- **Microservices + event-driven** (choreography over RabbitMQ), with a **storage tier** (coordinator + shard nodes) underneath.
- **Hexagonal (ports and adapters)** inside every service: `api/` (HTTP adapters) → `domain/` (pure logic: state machines, matching) → `adapters/` (coordinator client, AMQP, SQLite).

### 4.2 Component diagram

```
                         ┌──────────────────── Kubernetes namespace: erm ─────────────────────┐
 Client (curl / script)  │                                                                     │
 ────────HTTP──────────▶ │  Ingress (API Gateway)                                              │
                         │   /requests ─▶ request-service ×2      /resources ─▶ resource-service ×2
                         │   /match    ─▶ matching-service ×2                                   │
                         │                                                                     │
                         │   request-service ──cmd: match.request──▶ ┌──────────┐              │
                         │                                           │ RabbitMQ │──▶ matching-service
                         │   all services ──events: request.*,──────▶│ (1 pod)  │──▶ resource-service
                         │                   resource.*              └──────────┘   (release on cancel)
                         │                                                                     │
                         │   matching-service ──HTTP (If-Match)──▶ resource-service / request-service
                         │                                                                     │
                         │   request/resource services ──HTTP──▶ coordinator ×1  (+ /dashboard) │
                         │                                         │  ▲ register / heartbeat   │
                         │            ┌────────────────────────────┼──┴──────────────┐         │
                         │            ▼                            ▼                 ▼         │
                         │   shard-0: [shard-0-0][shard-0-1]  shard-1: [..-0][..-1]  shard-2: [..-0][..-1]
                         │            (StatefulSet, 2 pods, each with SQLite on a PVC)          │
                         └─────────────────────────────────────────────────────────────────────┘
```

### 4.3 Services and their responsibilities

| Component | Owns | Replicas | K8s kind |
|---|---|---|---|
| **request-service** | Requests (`req:*` keys): create, get, status transitions, publish match commands, reconciler | 2 | Deployment |
| **resource-service** | Resources (`res:*` keys): register, list, status/capacity, reserve/release (`If-Match`), consumes cancel events | 2 | Deployment |
| **matching-service** | Matching logic only (no data of its own): consumes `match.request`, ranks candidates, runs the assignment saga; also serves `POST /match/{id}` | 2 (scalable) | Deployment |
| **coordinator** | Cluster map (partitions → shards → nodes, roles, health), routing, replication, failover, resync orchestration, dashboard | 1 (spec) | Deployment |
| **storage node** | A SQLite KV store with versions, local secondary index on (kind, type, status) | 3 shards × 2 | StatefulSet per shard |
| **RabbitMQ** | Broker (+ management UI) | 1 (spec) | StatefulSet |

Each service owns its data (**database-per-service**, done logically through key namespaces). Matching never writes to storage directly; it goes through the owning service's API.

### 4.4 Data model

**Request** (`req:<uuid>`)
```json
{ "id": "…", "type": "water|food|medicine|transportation|shelter",
  "location": {"lat": 19.07, "lon": 72.87, "label": "Kurla West"},
  "details": "4 people, no drinking water", "urgency": 1-5, "quantity": 4,
  "status": "PENDING", "assigned_resource_id": null,
  "match_attempts": 0, "history": [{"status": "PENDING", "at": "…"}],
  "created_at": "…", "updated_at": "…", "version": 3 }
```

**Resource** (`res:<uuid>`)
```json
{ "id": "…", "type": "water", "name": "Tanker MH-01", "provider": "VOLUNTEER|NGO|SUPPLY",
  "location": {"lat": …, "lon": …, "label": "…"},
  "availability": {"quantity_total": 40, "quantity_remaining": 36},
  "status": "AVAILABLE|EXHAUSTED|OFFLINE",
  "allocations": [{"request_id": "…", "quantity": 4}],
  "created_at": "…", "updated_at": "…", "version": 7 }
```
Quantity is **one integer on one record**. The units are interchangeable (one food packet is as good as another), so they don't need their own IDs, the same way a shop tracks "40 in stock" rather than 40 records. Things that are *not* interchangeable (a particular volunteer, a particular ambulance) are registered as separate resources with quantity 1. The `allocations` list records who got how much, so that a cancel can give exactly that amount back.

**Request state machine** (State pattern; an invalid transition → `409`)
```
PENDING ──match──▶ ASSIGNED ──▶ IN_PROGRESS ──▶ FULFILLED
   │  └─retries exhausted─▶ UNMATCHED ──match/new supply──▶ ASSIGNED
   └──────────────┬─────────────┬──────────────┬─▶ CANCELLED  (from PENDING/UNMATCHED/ASSIGNED/IN_PROGRESS;
                                                               releases capacity if assigned)
```
Clients may set `IN_PROGRESS`, `FULFILLED` and `CANCELLED`. Only the matching saga sets `ASSIGNED`.

**Resource status** is derived: `quantity_remaining > 0 → AVAILABLE`, `= 0 → EXHAUSTED`. `OFFLINE` is set manually and overrides both.

---

## 5. Storage tier (the distributed-systems core)

### 5.1 Partitioning — DDIA ch. 7
- `partition = sha1(key) mod 12` (**12 fixed logical partitions**). Then `shard = partition_map[partition]`; initially partition `p` is assigned to shard `p mod 3`.
- We use a stable hash (sha1), never Python's `hash()`, which is randomised per process.
- Why not plain `hash mod 3`? Changing the shard count would move about 75% of keys. With fixed partitions, only whole partitions move (the Riak/Elasticsearch approach). Rebalancing itself is out of scope, but the design is ready for it.
- **Terminology warning**: DDIA 2e uses *shard* for a piece of the data, and a node can hold many shards. Our spec uses *shard* for a replicated **group of nodes**. So our 12 *partitions* are what DDIA calls shards, and our 3 *shards* are what DDIA calls nodes (each replicated). DDIA calls this technique a "fixed number of shards".
- This still satisfies the spec: it is hash-based partitioning, `hash(id) mod 12` followed by a table lookup instead of `mod 3`.

### 5.2 Storage node
- SQLite table `kv(key PK, kind, type, status, value JSON, version INT, deleted BOOL, updated_at)`, with an index on `(kind, type, status)`. This is a **local secondary index**.
- **Conditional write**: `PUT /internal/data/{key}` with `expected_version` → `409` on mismatch. On success, the primary sets `version + 1`.
- **Replica apply**: `PUT` with `mode=replica, version=v` is applied only if `v` > the stored version.
- Deletes are **tombstones** (versioned), so a resync cannot bring a deleted key back.
- `GET /internal/digest` returns `{key: [version, sha1(value)]}`, used for anti-entropy.

### 5.3 Replication — DDIA ch. 6 (single-leader, synchronous)
- Each shard has a **primary** and a **replica**, and the coordinator decides which is which. Nodes never promote themselves, so a split brain between nodes cannot happen.
- Write path (the coordinator is the replication pipe): write to the primary (CAS, which assigns the version) → forward the same `(value, version)` to the replica → acknowledge the client **after both succeed**.
- If the replica write fails: acknowledge anyway, mark the replica **STALE** (degraded mode, writes accepted with one copy). It gets resynced later.
- Reads go to the **primary** only (read-your-writes).

### 5.4 Failure detection and failover — DDIA ch. 9 (unreliable networks, timeouts)
- Heartbeat every **2 s**. Health states: `ALIVE → SUSPECT (>4 s) → DEAD (>6 s)`.
- **Fast path**: if a request to the primary fails (connection error or timeout), the coordinator immediately marks it SUSPECT, **promotes the in-sync replica**, and retries the operation once. The client sees no error.
- If both nodes of a shard are down, operations on that shard return `503`. Scatter-gather queries return `partial: true`.

### 5.5 Recovery — DDIA ch. 6 "catch-up recovery"
1. The node restarts with its PVC (old data intact) and calls `register` again. The coordinator marks it **RECOVERING**: it receives live writes but serves no reads.
2. The coordinator calls `POST /internal/sync {source: current primary}`. The node compares digests and pulls only the keys that differ (a **delta**, not a full copy). Keys that exist only on the recovering node are **discarded**, because they were never acknowledged and the primary is authoritative.
3. When the digests match, the node is marked **REPLICA (in sync)**. There is no automatic fail-back, to avoid flapping.

### 5.6 Coordinator restart (soft state, Tier 1 because it reuses the §5.5 machinery)
The cluster map is **rebuilt from heartbeats**: nodes re-register within about 2 s. For each shard, the coordinator elects a primary (the node with the highest total version) and resyncs the other node. K8s restarts the coordinator pod, so the single coordinator required by the spec is not a permanent point of failure. Weakness to state in the viva: while the coordinator is down (about 5 s), storage operations return `503`.

### 5.7 Internal APIs

| Where | Endpoint | Purpose |
|---|---|---|
| coordinator | `POST /internal/nodes/register`, `POST /internal/nodes/heartbeat` | membership |
| coordinator | `PUT/GET/DELETE /internal/data/{key}` (`If-Match` supported) | routed KV |
| coordinator | `GET /internal/query?kind=&type=&status=` | scatter-gather across 3 primaries |
| coordinator | `GET /internal/cluster`, `GET /internal/locate/{key}`, `GET /dashboard` | observability |
| storage node | `PUT/GET/DELETE /internal/data/{key}`, `GET /internal/query`, `GET /internal/digest`, `POST /internal/sync`, `GET /internal/stats` | node-local |

---

## 6. Messaging and matching

### 6.1 RabbitMQ topology
| Exchange | Type | Routing key → Queue | Notes |
|---|---|---|---|
| `erm.commands` | direct | `match.request` → `matching.q` | durable, `x-max-priority=5` (urgency), DLX = `erm.dlx`, manual ack, prefetch 10 |
| `erm.retry` | direct | → `matching.retry.q` | per-message TTL (5 s, 10 s, 20 s…), dead-letters back to `erm.commands` |
| `erm.events` | topic | `request.cancelled` → `resource.release.q`; `resource.registered` → `matching.wakeup.q`; `#` → `audit.q` | choreography and the event feed |
| `erm.dlx` | fanout | → `matching.dlq` | poison messages, inspected manually |

### 6.2 Asynchronous matching flow (saga)
1. request-service stores the request (`PENDING`), then publishes `match.request {request_id, type, urgency}` with priority = urgency and the correlation ID.
2. matching-service consumes the message. **Idempotency check**: if the request is no longer PENDING/UNMATCHED → ack and skip.
3. It fetches candidates: `GET resource-service/resources?type=X&status=AVAILABLE` (scatter-gather across shards).
4. The **MatchingStrategy** drops candidates with `quantity_remaining < request.quantity` or outside the max radius (default 50 km), then ranks the rest (§6.3).
5. **Reserve** the top candidate: `POST /resources/{id}/reserve {request_id, quantity}` with `If-Match: version` → `quantity_remaining −= quantity`, append to `allocations`. On `409` (someone else changed it first), re-fetch and try the next candidate.
6. **Assign** the request: PENDING → ASSIGNED with `If-Match`. If that fails (e.g. the request was cancelled meanwhile) → **compensate**: release the reserved quantity.
7. Publish `request.assigned`, then ack.
- No candidate → Tier 1: mark `UNMATCHED` and ack. Tier 2: republish to the retry queue with backoff, and mark `UNMATCHED` only after 5 attempts.
- An unexpected exception → `nack(requeue=false)` → DLQ.
- *(Tier 2)* `resource.registered` → matching re-queues `UNMATCHED` requests of that type (**wake-up**).
- *(Tier 2)* **Reconciler** (request-service, every 15 s): re-publishes requests that have been `PENDING` for more than 30 s. This covers messages lost between storing a request and publishing it. Safe because the consumer is idempotent.

### 6.3 Matching strategies (Strategy pattern, `MATCH_STRATEGY` env)
- `nearest`: minimum haversine distance; ties broken by more remaining capacity.
- `balanced` (default): `score = distance_km × (1 + load)` where `load = 1 − remaining/total`. This spreads requests away from providers that are almost full.

---

## 7. Design decisions (ADRs)

| # | Decision | Alternatives considered | Why |
|---|---|---|---|
| 01 | **Python 3.12 + FastAPI**, httpx, aio-pika, pydantic, managed with **uv** (workspace + lockfile) | Spring Boot, Node/Express | About 13 pods on one laptop: roughly 70 MB and under 1 s startup per Python pod, against roughly 300 MB and 5–15 s for Spring Boot. Fast startup also makes the pod-recovery and failover demos quick. Less boilerplate for a 3-day build; free OpenAPI `/docs`. The services are I/O-bound, so the GIL doesn't matter |
| 02 | **3 microservices**: request, resource, matching | 2 services (request+matching) | Clear ownership (SRP); matching stays stateless and independently scalable; shows service-to-service calls |
| 03 | **12 fixed partitions → 3 shards** | `mod 3`; consistent-hash ring | Rebalance-ready with little extra complexity (DDIA ch. 7) |
| 04 | **SQLite per node on a PVC** | In-memory dict; Postgres/Redis per shard | Durable, zero-ops, gives a real local index. A managed DB would replicate itself and hide the part we're meant to build |
| 05 | **Single-leader, synchronous, coordinator-driven replication** | Async; leaderless quorum | No acknowledged-write loss on failover; quorum needs ≥ 3 copies per shard |
| 06 | **Optimistic concurrency** (versions, `If-Match`, 409) | Distributed locks; pessimistic locking | No lock leases or deadlocks; conflicts are rare and cheap to retry (DDIA ch. 8, "lost updates") |
| 07 | **Heartbeat + fail-fast on error** | Heartbeat only | Failover within one request instead of a 6 s outage |
| 08 | **Delta resync by digest, primary wins** | Full copy; Merkle trees | Simple, correct with tombstones; fast because PVCs keep old data |
| 09 | **Saga with compensation** for reserve → assign | 2PC | No blocking coordinator for transactions; fits microservices |
| 10 | **Priority + TTL-retry + DLQ** topology | Single plain queue | Urgent requests first; no busy-looping on scarcity; poison messages isolated |
| 11 | **Reconciler + idempotent consumer** | Transactional outbox | Outbox needs an atomic write across storage and queue; the reconciler gives the same guarantee (eventual) |
| 12 | **StatefulSet per shard**, Deployments for stateless services, Ingress gateway | Deployments everywhere | Storage needs a stable identity (`shard-1-0`) and its own volume |
| 13 | **JSON logs + correlation ID + live dashboard** | Prometheus/Grafana stack | Highest demo value for the time; Prometheus `/metrics` is a stretch goal |
| 14 | **Coordinator soft state** rebuilt from heartbeats | Persist the cluster map | Nothing to corrupt; restart is self-healing |
| 15 | **Location is data, not placement**: the client sends lat/lon in the body; keys are sharded by hash of ID; matching scatter-gathers candidates and ranks by haversine distance | Geo-sharding (shard by region or geohash); global secondary index on (type, geohash cell) | A disaster concentrates load in one region, so geo-sharding would overload exactly one shard when it matters most. Hashing spreads the load evenly. Scatter-gather is cheap at our scale; a global geo index is how this would scale (DDIA ch. 7) |
| 16 | **Retry with exponential backoff + jitter** when storage is unavailable (0.2 s doubling to a 2 s cap, 5 retries); `Retry-After: 2` header on every 503 for outside clients | Fail immediately; retry at a fixed interval | Immediate or fixed-interval retries from every caller hit a recovering component at the same moments (retry storm). Doubling the wait reduces load while it recovers; jitter stops callers retrying in lockstep. Only "not applied" errors (503, connect refused) are retried; a timeout mid-request returns 504 because the outcome is unknown |

### Design patterns used (SADP)

| Pattern | Where |
|---|---|
| API Gateway | K8s Ingress path routing |
| Hexagonal / Ports and Adapters | Inside every service |
| Repository | `StorageRepository` port; coordinator-client adapter |
| Strategy | `MatchingStrategy` (`nearest`, `balanced`); `Partitioner` |
| State | Request lifecycle transition table |
| Publish–Subscribe / Observer | `erm.events` topic exchange |
| Competing Consumers | Matching pods sharing `matching.q` |
| Saga (compensating transaction) | Reserve → assign → (release) |
| Optimistic Concurrency Control | Versions + `If-Match` / 409 |
| Idempotent Consumer | Matching skips non-PENDING requests |
| Retry with Backoff + Dead Letter Channel | `common/retry.py` (storage calls, ADR-16), `erm.dlx`; `erm.retry` queue is Tier 2 |
| Leader–Follower (primary–backup) | Shard replication |
| Heartbeat | Node liveness |
| Anti-entropy | Digest-based resync |
| Reconciliation loop | PENDING sweeper (same idea as K8s controllers) |
| Correlation Identifier | `X-Correlation-ID` through HTTP and AMQP |
| Health Check | `/health` (liveness), `/ready` (readiness) probes |

SOLID: SRP (one responsibility per service and module), OCP (new strategy = new class, no edits elsewhere), DIP (domain depends on ports, not on httpx/aio-pika).

---

## 8. Deployment, testing, demo

### 8.1 Kubernetes (`k8s/`)
- Namespace `erm`. ConfigMap (heartbeat timings, partitions, strategy, radius). Secret (RabbitMQ creds).
- `rabbitmq` StatefulSet + Service (5672, 15672).
- `coordinator` Deployment (1) + Service. `shard-{0,1,2}` StatefulSets (replicas 2, `volumeClaimTemplates`) + a headless Service each.
- `request-`, `resource-`, `matching-service` Deployments (2 each) + ClusterIP Services + Ingress. Liveness/readiness probes on all.
- Stretch: HPA on matching-service (needs the metrics-server addon).
- `docker-compose.yml` with the same topology, for local development.

### 8.2 Tests
- **Unit**: partitioner distribution, state transitions, strategies, haversine, version/CAS rules.
- **Integration** (compose): every Table 2 endpoint and status code; async create → ASSIGNED.
- **Concurrency**: 50 simultaneous requests (quantity 1) vs total quantity 10 → exactly 10 ASSIGNED, 0 oversold.
- **Failover**: kill the primary during a write loop → no acknowledged write lost, no client errors.
- **Recovery**: restart the node → digests equal, node back as in-sync REPLICA.

### 8.3 Demo scripts (`scripts/demo/`, one per item in spec §10)
`01-create-get` · `02-register` · `03-match` · `04-distribution` (locate + per-shard counts) · `05-async-burst` (100 requests, watch the queue drain) · `06-kill-storage` · `07-recover-storage` · `08-k8s-pod-recovery` · `09-k8s-scale`. The dashboard stays open alongside.

### 8.4 Plan and handover

**Phase A: Claude + Sarthak, until handover (~1.5 days, about 11-17 morning)**
1. uv workspace, `common` package (models, partitioner, logging, correlation ID).
2. Storage node + coordinator: routing, replication, heartbeats, failover, resync, `/locate`, dashboard. Storage tests.
3. request-, resource- and matching-service + RabbitMQ (Tier 1 topology: priority queue, DLQ). Saga with cancel compensation.
4. docker-compose with everything running end to end; unit, integration and concurrency tests passing.
5. K8s manifests (first version, smoke-tested on minikube).
6. `HANDOVER.md` (what's done, how to run, what's left) + Commands section in CLAUDE.md filled in.

**Phase B: friend, after handover**
1. Harden the minikube deployment (Ingress addon, probes, scaling).
2. Demo scripts for spec §10 (some written in Phase A).
3. Tier 2 features, in this order: retry/backoff queue → wake-up on new supply → reconciler → `balanced` strategy → dashboard queue depths → HPA.
4. Report/slides (DESIGN.md is the base).

---

## 9. Decisions log

| Question | Decision |
|---|---|
| Language | Python 3.12 + FastAPI, uv (ADR-01) |
| Resource capacity | Quantity on resource and request, no splitting (FR-17) |
| Partitioning | 12 fixed partitions → 3 shards (ADR-03). Sarthak to confirm after reading DDIA ch. 7 |
| Scope | Tier 1 now, Tier 2 progressively (§2.2, §8.4) |
| Gateway | minikube Ingress addon |
| Team | Friend takes over at ~1.5 days (§8.4) |
| Friend and Python | Fine; friend works with Claude Code using CLAUDE.md + HANDOVER.md |
| Freeze | Approved 2026-11-16 |

---

## 10. Implementation notes (post-freeze, 2026-11-16)

These are clarifications made while building. None of them changes an ADR's decision, but each is listed so it can be defended.

| # | What | Where it differs from the text above | Why |
|---|---|---|---|
| N1 | **Reserve retries close to the data.** matching-service calls `reserve` *without* `If-Match`; resource-service does read → check quantity → compare-and-set write, re-reading on conflict (up to 20 times, with jitter). `If-Match` is still accepted on `reserve` and on both public status endpoints | §6.2 step 5 said matching sends `If-Match` and re-queries on 409 | The guarantee is identical (the storage CAS refuses stale writes), but a retry costs one GET + PUT instead of a whole scatter-gather query. Verified: 50 concurrent requests vs 10 units → exactly 10 assigned, 64 conflicts resolved |
| N2 | **Transient errors are requeued.** If a consumer gets `503` (e.g. a shard failing over), it waits 1 s and requeues the message. Only real bugs go to the DLQ | §6.2 said "unexpected exception → DLQ" | A failover is expected and lasts under a second; dead-lettering those messages would lose work |
| N3 | **Election safety rule.** A shard without a primary elects the live node with the highest `version_sum`, *unless* a DEAD node of that shard reported a higher one. Then it stays unavailable until that node returns | §5.6 only described the coordinator-restart case | Prevents a STALE replica from silently losing acknowledged writes in a double failure (consistency over availability) |
| N4 | **`balanced` strategy is already implemented** (Tier 2 item done early) | §2.2 marked it Tier 2 | 10 lines of code; unit-tested |
| N5 | **One Dockerfile, `SERVICE` build arg**, one image per service. On minikube, images are built on the host and loaded with `minikube image load` | §8.1 didn't specify | The existing minikube profile uses containerd, where `minikube docker-env` doesn't work |
| N6 | **Local gateway**: docker-compose runs nginx on :8080 with the same path routing as the Ingress; the coordinator dashboard is on :8090 | §8.1 | Same client experience locally and on Kubernetes |
| N7 | **Measured performance.** Create request: p50 38 ms, p95 47 ms (sequential, minikube, through the Ingress). Under a 20-way parallel burst against *one* resource, p95 rises to about 500 ms | NFR-6 | Updates to a single hot record are serialised by design (that's what prevents overselling). Load spread over several resources scales with matching pods (demo 9) |
| N8 | **ADR-16 added** (Sarthak's proposal): exponential backoff with jitter on storage calls, `Retry-After` on 503 | new | Verified on minikube: coordinator pod deleted during 30 requests → 28 succeeded, 1 × 504 (in flight when the pod died), 1 × 503 after retries ran out. No before/after comparison was measured |
| N9 | **Chaos test** (`scripts/chaos.py`, `make chaos`): client traffic + random hard kills of storage pods, then 6 automatic checks (durability, durable cancels, no over-allocation, depots ↔ requests agree, no stuck requests, replicas identical). New coordinator endpoint `GET /internal/verify` compares every replica's digest with its primary's; the dashboard shows it per shard as "copies identical" | new (beyond the spec) | Turns our guarantees into evidence. On minikube 2026-11-17: normal run 16 kills (10 primaries) in 2 min, 5,658/5,658 writes accepted, all 6 checks pass. `--aggressive` run 27 kills (19 primaries) in 93 s, 4,072/4,072 accepted, all 6 pass |
| N10 | **Landing page with a live map** at `/` (served by request-service, routed by the Ingress): live counters, forms to register resources and create requests, a map (Leaflet + OpenStreetMap tiles) showing resources with units left, requests by status and a dashed line for each match; clicking the map picks a location. `GET /requests` gained `limit` and `by_status` so the page fetches only the newest 200. `scripts/demo/00-seed-mumbai.sh` adds realistic Mumbai data | new (beyond the spec) | Makes "nearest resource with enough quantity" visible. The map library and tiles come from the internet; without it the page shows a notice and everything else works |
| N11 | **Tier 2 implemented (branch `feature/tier2`, to be tested before merging):** retry with backoff, wake-up on new or freed supply, reconciler, queue depths on the dashboard, HPA for matching-service. Two details differ from §2.2/§6.2: (a) a request becomes **UNMATCHED on its first failed attempt**, not after the 5th, and the retries continue in the background; (b) the retry uses **one queue per delay** (`matching.retry.2s` … `32s`, each with a queue-level TTL) instead of one queue with per-message TTLs. Wake-up also reacts to *restocked* and *released* supply, not only *registered* | §2.2 FR-19, §6.1, §6.2 | (a) The person sees "waiting for supply" immediately, and the reconciler (which re-sends requests stuck in PENDING) can't mistake a request that is merely waiting for a lost one. (b) RabbitMQ only expires messages at the head of a queue, so a 2 s retry behind a 32 s one would wait 32 s (head-of-line blocking). (c) Cancelling frees quantity, which is new supply too |
| N12 | **Bugs found by the chaos test and fixed** (2026-11-17, while testing `feature/tier2`). The Tier 2 features added load, which exposed bugs that the unloaded runs on `main` never triggered. Several of them were already in `main`. Each one is fixed and, where possible, covered by a unit test that fails on the old code. **Storage tier:** (1) a resync finishing set IN_SYNC even if the node had become STALE meanwhile, giving a replica that silently lacked writes; (2) when a primary died with no in-sync replica, a STALE replica could win the `version_sum` election, and the returning primary was resynced *from* it, deleting acknowledged writes (the shard now waits for the previous primary); (3) a write racing a failover was applied only on the old primary but still acknowledged (it now replicates to every node except the writer, and returns 503 unless the current primary has it); (4) a read timeout triggered failover of a node that was only slow (now only refused/reset connections do; a timed-out write is read back and replicated, "read repair"); plus background anti-entropy as a safety net. **Saga:** a timed-out assign was treated as failed and its reservation released even when the assign had succeeded (now: retry idempotent steps, re-read on conflict, compensate only on definite failure, resume from a held reservation). **Messaging:** cancels of never-assigned requests were dead-lettered; 504s were dead-lettered instead of retried; publishing hung while RabbitMQ was down; retries starved fresh requests (retries now use priority 0); wake-up re-matched the same 50 requests on every event (now bounded by the freed units and coalesced). **HTTP:** server keep-alive shorter than the clients' caused 502s and false failovers (now 75 s) | §5, §6 | Final results on minikube: 3 normal chaos runs in a row (up to 15 kills each) plus 1 aggressive run (24 kills in ~90 s), all with every check passing and every write accepted; 50 tests pass. Idle create latency p50 23 ms / p95 51 ms. Under chaos the single coordinator process becomes the bottleneck (about 1 CPU core) and latency rises to around 1 s |
| N13 | **New frontend** at `/` (branch `feature/frontend`, replaces the earlier landing page; still one self-contained file served by request-service, no new pod, only the public API). Two guided flows in plain language: **I need help** (pick what, how many, how urgent, where; then a live tracker with progress steps, the helper's name and distance, cancel / "help has arrived") and **I can help** (offer supplies; pause, restock, see who you are matched to). Also a **Live board** (counts, activity feed, demo tools) and a **System** tab embedding the storage dashboard. Before submitting, a live hint says whether anyone nearby can help (client-side haversine, 50 km like the matcher). The page retries 503s automatically (the backend's `Retry-After` contract, ADR-16) and never retries a POST after a dropped connection | new (beyond the spec) | Makes the system understandable to a non-technical audience. UI bugs found and fixed while testing with a scripted browser: slow poll responses overwriting newer state (now ordered by history length / `updated_at`), panels rebuilt on every poll swallowing clicks (now only touched when content changes), and a skipped "matched" toast when a match was faster than the first poll |

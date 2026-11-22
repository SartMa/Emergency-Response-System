# How the system works, under the hood

A guided tour of the finished system for someone who has to understand it, explain it and defend it. `DESIGN.md` says **what was decided and why**; this file says **how it actually behaves**, step by step, and links every idea to the code. Read it with the code open.

Contents: 1 Big picture · 2 Building blocks · 3 One request, step by step · 4 Storage tier · 5 Services · 6 RabbitMQ · 7 Cross-cutting plumbing · 8 Docker and Kubernetes · 9 Frontend · 10 "What happens when X dies" · 11 The bugs chaos testing found · 12 Patterns map · 13 Limitations · 14 Viva questions · 15 Reading order

---

## 1. The big picture

```
 browser / curl
      │  HTTP
      ▼
 ┌──────────────┐   /requests            ┌──────────────────┐
 │ Ingress      │ ─────────────────────▶ │ request-service  │ ×2 pods
 │ (nginx)      │   /resources           ├──────────────────┤
 │ API gateway  │ ─────────────────────▶ │ resource-service │ ×2 pods
 │              │   /match               ├──────────────────┤
 │              │ ─────────────────────▶ │ matching-service │ ×2..5 pods (HPA)
 └──────────────┘                        └───────┬──────────┘
                                                 │ every read/write of data goes via HTTP to ▼
        RabbitMQ  ◀──publish/consume──  services │
        (1 pod)                           ┌──────▼──────────┐
                                          │  coordinator    │ ×1 pod: routing, replication,
                                          │  (the brain)    │ failure detection, failover
                                          └──┬────┬────┬────┘
                                             ▼    ▼    ▼
                                       shard 0  shard 1  shard 2     each shard = 2 pods:
                                       [P] [R]  [P] [R]  [P] [R]     primary (P) + replica (R),
                                                                      each with its own SQLite file on a volume
```

Two ideas run through everything:

1. **The data lives in a home-made distributed key-value store** (coordinator + 6 storage nodes). The three services are *clients* of that store. They never know which node holds what.
2. **Services talk in two ways:** synchronous HTTP (when the caller needs the answer now) and RabbitMQ messages (when work can happen later). Creating a request is synchronous; *matching* it is asynchronous.

---

## 2. The building blocks

| Block | Code | What it owns | State | Scaling |
|---|---|---|---|---|
| **request-service** | `services/request_service/` | the request lifecycle (PENDING → ASSIGNED → …) | none (stateless) | 2 pods; add more freely |
| **resource-service** | `services/resource_service/` | resources, their quantity, reservations | none | 2 pods |
| **matching-service** | `services/matching_service/` | the *decision* of who helps whom | none | 2–5 pods, autoscaled |
| **coordinator** | `services/coordinator/` | the map of the cluster: who is primary, who is alive, who is in sync | in memory only (rebuilt from heartbeats) | exactly 1 (spec) |
| **storage node** ×6 | `services/storage_node/` | a SQLite file: keys → versioned JSON | on a persistent volume | 3 shards × 2 |
| **RabbitMQ** | `services/common/amqp.py` (client) | queues of messages | on a volume | 1 (spec) |
| **common library** | `services/common/` | config, logging, errors, retry, partitioner, HTTP + AMQP helpers | – | shared code |

Rule: **services own their data logically, not physically.** request-service only touches keys starting `req:`, resource-service only `res:`. matching-service owns *no data at all*; it asks the other two to change things through their APIs.

---

## 3. One request, step by step

Scenario: a family in Kurla West asks for 2 units of water, urgency 3. A volunteer with 2 units stands 0.36 km away.

1. **Ingress.** The browser sends `POST /requests`. nginx routes by path to one of the two request-service pods. The service's middleware (`common/http.py`) creates a **correlation ID** (or keeps the one sent in `X-Correlation-ID`) and stores it in a context variable. Every log line and every outgoing call from now on carries it.
2. **Validation and creation.** Pydantic checks the body (400 if wrong). `domain.new_request` builds the request: a UUID, status `PENDING`, `history: [PENDING]`.
3. **Store it.** `StorageClient.create` sends `PUT /internal/data/req:<uuid>` with `expected_version: 0` ("create, must not exist") to the coordinator.
4. **The coordinator stores it.** (§4.3 in detail) It hashes the key with sha1 into one of 12 partitions, looks up the shard, writes to that shard's **primary** (a compare-and-set that yields version 1), copies the same value and version to the **replica**, checks that the right node has it, and only then answers.
5. **Queue it.** request-service publishes `{request_id, type, urgency}` to RabbitMQ (exchange `erm.commands`, key `match.request`), as a persistent message with **priority = urgency** and the correlation ID attached. If RabbitMQ doesn't answer within 2 s the request is still stored; the reply says `queued_for_matching: false` and the reconciler (§5.5) fixes it later.
6. **Reply.** `201 Created`, `ETag: "1"` (the storage version), `Location: /requests/<id>`. The person has an answer in ~25 ms. Nothing has been matched yet.
7. **A matching pod wakes up.** All matching pods consume the same queue; RabbitMQ gives each message to exactly one of them (competing consumers). It restores the correlation ID from the message.
8. **Read the facts.** The saga reads the request (`GET` request-service → coordinator → primary of that shard) and lists resources of type water (`GET /resources?type=water` → resource-service → coordinator `/internal/query`). That query is **scatter-gather**: the coordinator asks the primaries of all three shards in parallel and merges the answers. Each shard answers from a SQLite index on (kind, type, status).
9. **Rank.** `strategies.py` drops resources with fewer units than needed or farther than 50 km (haversine distance), then sorts by distance (ties by ID, so it's deterministic).
10. **Reserve.** The saga calls resource-service `POST /internal/resources/<id>/reserve {request_id, quantity}`. There, `read → domain.reserve → write with expected_version` runs. If two matchers race for the same units, the storage layer accepts one write and rejects the other with 409; the loser re-reads (now with fewer units) and re-checks. The reservation is recorded in the resource's `allocations` list, keyed by request ID.
11. **Assign.** The saga calls request-service `POST /internal/requests/<id>/assign` with `If-Match: <version it read>`. request-service moves the request `PENDING → ASSIGNED`, stores the assignment (resource ID, quantity, distance) and an entry in `history`, writes it with compare-and-set, and publishes a `request.assigned` event.
12. **Ack.** The saga returns "matched", the consumer **acks** the message (only now does RabbitMQ forget it), and logs `matched`.
13. **The browser sees it.** The tracker polls `GET /requests/<id>` every 1.2 s, sees `ASSIGNED`, loads the helper's name and shows "Help is on the way".

Cost of one match: about 3 reads and 2 replicated writes to storage, 1 queue message in, 1 event out. Follow it in the logs with the correlation ID:
`kubectl -n erm logs -l app=matching-service --tail=-1 | grep <correlation-id>`.

**If nobody can help** (step 9 finds nothing): the request becomes `UNMATCHED` immediately ("waiting for supply") and the message is re-published to a delay queue to retry after 2, 4, 8, 16 and 32 s. Meanwhile, if someone registers a matching resource, the request is woken up at once (§5.5).

**If the person cancels:** `PUT /requests/<id>/status {CANCELLED}` → request-service writes it and publishes `request.cancelled` → resource-service consumes it and **releases** the reserved units (the saga's compensation) → announces `resource.released` → matching wakes up waiting requests that can now be served.

---

## 4. The storage tier in depth

This is the part the course is about. Source: `services/common/partitioner.py`, `services/storage_node/store.py`, `services/coordinator/cluster.py` (rules, no I/O) and `services/coordinator/router.py` (the I/O).

### 4.1 Partitioning: where does a key live?

`partition = int(sha1(key)[:8]) mod 12`, then `shard = partition_map[partition]`. The map is a plain table: partition p → shard p mod 3, so each shard owns 4 partitions.

- We use **sha1, not Python's `hash()`**, because `hash()` is randomised per process: two services would disagree about where a key lives.
- **Why 12 partitions and not `hash mod 3`?** If you ever go from 3 to 4 shards, `mod 3 → mod 4` moves ~75% of all keys. With fixed partitions you hand over whole partitions: 3 of 12 move, 25%, the minimum possible. A unit test (`test_fixed_partitions_move_fewer_keys_than_mod_n`) demonstrates it. (The technique is usually called "a fixed number of partitions". Careful with the words: elsewhere *shard* often means a piece of data, while in this project a *shard* is a replicated group of nodes.)
- Location is **data, not placement**: a request in Kurla and a resource 1 km away usually live on different shards. That is fine, because matching *scatter-gathers* the candidates. Sharding by region would put all the load of one flood on one shard (ADR-15).

### 4.2 One storage node

A node (`store.py`) is a SQLite file with one table: `key, kind, type, status, value (JSON), version, deleted, updated_at`, and an index on `(kind, type, status)`. That index is a **local secondary index**: each node can answer "available water resources" for the keys *it* holds; the coordinator merges the answers.

Three properties make everything else possible:

- **Versions.** Every key has a version that only grows. A primary write is a **compare-and-set**: "write this if the stored version is exactly N" (`0` means "create"). Otherwise `VersionMismatch` → HTTP 409. This is the foundation of all concurrency control (§5.1).
- **Tombstones.** A delete is a versioned marker (`deleted=1`), not a removed row, so a later resync can't resurrect a deleted key.
- **Digest.** `GET /internal/digest` returns `{key: [version, hash-of-value, deleted]}` for every key. Comparing two digests tells you precisely which keys differ. Used by resync, the verify endpoint and anti-entropy.

A node is deliberately **dumb**: it doesn't know if it's primary or replica. The coordinator tells it what to do (a primary-style `PUT` with a version check, or a replica-style `POST /internal/replicate` carrying the primary's version, applied only if newer). Because nodes never promote themselves, **two primaries can never exist** (no split brain).

### 4.3 The write path (single-leader, synchronous replication)

`Router._put` in `router.py`:

1. Find the shard (§4.1) and its current **primary**.
2. `PUT` to the primary with `expected_version`. The primary does the compare-and-set and returns the new version. (409 → the client gets `VERSION_CONFLICT`.)
3. `_replicate`: send the same `(key, value, version)` to **every other usable node of the shard**, in parallel, and wait. A follower that fails is marked **STALE** (degraded mode: the write still succeeds with one copy).
4. **Final safety check:** if the primary *changed* while the write was in flight (a failover raced with it), the write is acknowledged only if the *new* primary confirms it applied it. Otherwise the client gets 503 and retries. (This was bug #3 in §11.)
5. Acknowledge the client.

So an acknowledged write is on the primary **and** on the replica, unless the replica was already known to be broken. The whole thing runs inside `asyncio.shield`, so a client hanging up mid-write can't leave it half-done.

Synchronous vs asynchronous replication: synchronous means no acknowledged write is lost when the primary dies, at the cost of write latency and of reduced guarantees in degraded mode.

### 4.4 The read path

Reads go to the **primary only**. That gives read-your-writes: after a write is acknowledged, any read sees it. (Reading from the replica would be faster but could return stale data.) If the primary is unreachable the coordinator fails over and retries on the replica.

### 4.5 Cluster state: three independent attributes per node

`cluster.py`. This file is pure logic (no network, no clock), which is why it's testable.

| Attribute | Values | Meaning |
|---|---|---|
| **role** | PRIMARY / REPLICA | who serves reads and assigns versions |
| **health** | ALIVE / SUSPECT / DEAD | derived from heartbeats and failed requests |
| **sync** | IN_SYNC / STALE / SYNCING | does this node hold every acknowledged write? |

The **invariant** everything protects: *only an IN_SYNC node may be promoted to primary.* A STALE node is missing writes; promoting it would silently lose acknowledged data.

`sync` moves like this: a new/restarted/failed node is **STALE** → the repair loop starts a resync → **SYNCING** → digests match → **IN_SYNC**. If anything marks it STALE *during* the resync (a live write to it failed, or it was declared DEAD), finishing the resync does **not** make it IN_SYNC; it must resync again (`mark_in_sync` returns False).

### 4.6 Detecting failure (two paths)

- **Heartbeats.** Every storage node posts a heartbeat every 2 s (carrying key counts and its `version_sum`). The coordinator's monitor loop runs every 0.5 s: no heartbeat for >4 s → **SUSPECT**; >6 s → **DEAD** (and STALE; and if it was primary, failover). Any timeout is a guess; 6 s is "3 missed beats", a balance between slow detection and false alarms.
- **Fast path.** When a request to the primary fails with *connection refused/reset* (a killed pod), the coordinator doesn't wait for the heartbeat timeout: it fails over immediately and retries the operation once on the new primary. The client sees no error.
- **A slow node is not a dead node.** A *read timeout* on an open connection only means "no answer yet". The coordinator does **not** fail over on that (a chaos run showed that doing so multiplied failovers and load). It first does "read repair" for a write whose outcome is unknown, then answers 503 so the client retries.

### 4.7 Failover

`ClusterState.failover`: the failed primary becomes a STALE replica; the **IN_SYNC replica** (if any) is promoted. If no replica is in sync, the shard becomes unavailable (503 `SHARD_UNAVAILABLE`) and the coordinator *remembers* which node was the last primary, because that node (and its disk) holds every acknowledged write. The shard waits for it, rather than electing a stale replica. That's a deliberate **CP choice** (consistency over availability) for a rare double failure. Calling `failover` twice for the same failure is harmless (idempotent).

### 4.8 Recovery: how a restarted node catches up

1. Kubernetes restarts the pod; its **volume still has the old SQLite file**. The node registers with the coordinator, which marks it a STALE replica.
2. The coordinator's repair loop calls `POST /internal/sync {source_url: <primary>}` on the node.
3. The node fetches the primary's digest and compares it with its own. For every key whose version/hash differs it fetches the primary's copy and **overwrites its own, even if its own version is higher**: a higher local version means a write that was never acknowledged, and the **primary is authoritative**. Keys the primary doesn't have are deleted. Only differing keys are copied: a **delta**, not a full copy (that's why the dashboard says things like "resynced (4 keys copied)").
4. The coordinator compares both digests. Identical *and* node still SYNCING → **IN_SYNC**. Different (a live write raced with the sync) → sync again, up to 5 times.
5. The node does **not** take the primary role back (no flapping).


### 4.9 Elections and the coordinator's memory

The coordinator keeps the cluster map **in memory only** (soft state, ADR-14). If its pod is restarted: it starts in a 5 s "forming" period (all storage calls answer 503 `CLUSTER_FORMING`), the nodes notice they're unknown (their heartbeat gets 404) and **re-register**, and then it elects a primary per shard: the live node with the highest `version_sum` (a count of how many writes it has seen), unless a *dead* node reported a higher one. This heuristic is only for the restart case; it's weaker than a real replicated log, and it's listed as a limitation.

### 4.10 Anti-entropy and the verify endpoint

`GET /internal/verify` compares every node's digest to its primary's: "are the copies identical?" The dashboard shows it per shard ("copies identical") and the chaos test uses it as its sixth check. A background loop (every 30 s) uses the same comparison as a **safety net**: if an IN_SYNC replica differs from its primary on the **same key at the same version in two consecutive rounds**, it's marked STALE and resynced. (One round isn't enough: under constant writes there is always some key in flight.) This is the technique Dynamo and Cassandra use.

---

## 5. The services in depth

### 5.1 Optimistic concurrency: the idea that holds it all together

Problem (the classic "lost update"): two matchers read a resource with 6 units left, both subtract 4, both write "2 left". One subtraction is lost, and 8 units were handed out from 6.

Solution: no locks. Every record carries a **version**. A writer says "replace version 7 with this", and the store refuses if the current version isn't 7. The loser gets a conflict, re-reads (now version 8, 2 units left), and decides again.

You'll see it at three levels:
- **storage:** `expected_version` on every write (409 → `VERSION_CONFLICT`);
- **services:** `read → change in memory → write with expected_version → on conflict, re-read and retry` (up to 20 times with jittered backoff for hot resources);
- **HTTP:** the version is the `ETag`, and clients can send `If-Match`.

Why not locks? A lock needs a lease, can be held by a crashed process, and invites deadlocks. Conflicts here are rare and cheap to retry. Proved by the integration test: 50 simultaneous requests against 10 units → exactly 10 assigned.

### 5.2 request-service: the lifecycle as a state machine

`request_service/domain.py` has a transition table. Anything else is `409 INVALID_TRANSITION`.

```
PENDING ──match──▶ ASSIGNED ──▶ IN_PROGRESS ──▶ FULFILLED
   │ └─no resource──▶ UNMATCHED ──match──▶ ASSIGNED
   └────────┬───────────┬───────────┬──────▶ CANCELLED   (from any non-final state)
```

Clients can set `IN_PROGRESS`, `FULFILLED`, `CANCELLED`. Only matching sets `ASSIGNED` / `UNMATCHED`. Every change appends to `history` (the audit trail the tracker shows under "What happened so far").

### 5.3 resource-service: quantity, not units

A resource is one record: `quantity_total`, `quantity_remaining`, and `allocations: [{request_id, quantity}]`. Units are interchangeable (like "40 in stock"), so they need no IDs. Individually distinguishable things (one particular ambulance) are registered as separate resources with quantity 1.

- **Status is derived**, never stored independently: `online and remaining > 0` → AVAILABLE; `remaining = 0` → EXHAUSTED; switched off → OFFLINE. So it can't contradict the numbers.
- **reserve and release are idempotent per request ID.** Reserving twice for the same request changes nothing the second time; releasing a request that holds nothing is a no-op. This makes retries and redelivered messages safe.
- It also **announces supply**: `resource.registered`, `resource.restocked`, `resource.released` events (with location and units) feed the wake-up (§5.5).

### 5.4 matching-service: strategy and saga

**Strategy pattern** (`strategies.py`): hard rules first (enough quantity, within radius), then an ordering. `nearest` = smallest distance. `balanced` = distance × (1 + how depleted the provider is), which spreads load. Selected by an env var; adding one means one new class.

**Saga** (`saga.py`). Reserving on a resource and assigning a request involve two services and two shards, so they can't be one database transaction. Instead: *do step 1; do step 2; if step 2 definitively fails, undo step 1 (compensate)*.

The hard part is **timeouts**: "my assign call timed out" does *not* mean "the assign failed". It may have succeeded and only the answer was lost. The saga therefore never guesses:

- every step is **idempotent**, so on an unknown outcome it just **retries** the step (with backoff);
- on a conflict it **re-reads the truth**: if the request is already assigned to *this* resource, the earlier attempt succeeded;
- it **compensates only on a definite failure** (request cancelled, or assigned elsewhere);
- if the retries run out, the error propagates, the message goes back to the queue, and the next delivery **resumes** from the reservation already held (or cleans it up if the request no longer needs it).

### 5.5 Retry, wake-up, reconciler: three ways a waiting request gets help

1. **Retry with backoff.** No resource → `UNMATCHED` at once → re-published to `erm.retry`, which routes by delay to queues `matching.retry.2s … 32s`; each has a TTL, and when it expires RabbitMQ moves the message back to `matching.q`. After the 5th retry the request just stays UNMATCHED. Retries are published at priority **0**, so fresh requests always go first.
2. **Wake-up on new supply.** Supply events reach `matching.wakeup.q`. Events are **coalesced for 1 s per type**, then one scan lists UNMATCHED requests of that type and wakes only as many (most urgent first, within the radius) as the new units could actually serve. A release of 2 units wakes at most 2 one-unit requests.
3. **Reconciler.** Every ~15 s each request-service pod looks for requests still `PENDING` after 30 s (their match message must have been lost, e.g. RabbitMQ was down at creation) and re-sends them, at most once a minute each. Duplicates are harmless because the consumer is idempotent. This is the same idea as a Kubernetes controller: compare desired state with actual state and repair the difference.

---

## 6. RabbitMQ: topology and guarantees

| Exchange (type) | Routing key → queue | Purpose |
|---|---|---|
| `erm.commands` (direct) | `match.request` → `matching.q` | "match this request"; `x-max-priority 5` = urgency |
| `erm.retry` (direct) | `retry.2s … retry.32s` → `matching.retry.<N>s` | delayed retries; TTL = N s; expired messages dead-letter back to `matching.q` |
| `erm.events` (topic) | `request.cancelled` → `resource.release.q` | give quantity back |
|  | `resource.registered/restocked/released` → `matching.wakeup.q` | wake up waiting requests |
| `erm.dlx` (fanout) | → `erm.dlq` | dead letters: messages that failed for good |

**One queue per delay**, not one queue with per-message TTLs, because RabbitMQ only expires messages at the *head* of a queue: a 2 s retry stuck behind a 32 s one would wait 32 s.

**Delivery guarantee: at-least-once.** A message is removed only after the consumer **acks**. If the pod dies before acking, RabbitMQ redelivers it to another pod. So a handler may see the same message twice, which is why *every handler is idempotent* (matching skips requests that are no longer PENDING/UNMATCHED; reserve/release are idempotent per request).

Consumer outcomes (`common/amqp.py`): success → ack. 503/504 (dependency down or outcome unknown) → wait 1 s and **requeue**. Anything else → reject without requeue → **dead-letter queue** for inspection. The connection is "robust": it reconnects and re-subscribes by itself when RabbitMQ restarts, and publishing gives up after 2 s instead of hanging.

---

## 7. Cross-cutting plumbing (`services/common/`)

- **Correlation ID** (`logging.py`, `http.py`): created at the edge, carried in a context variable, added to every outgoing HTTP call (`X-Correlation-ID`) and AMQP message (the `correlation_id` property), and printed in every JSON log line. One ID traces a request across all services.
- **Structured JSON logs:** one JSON object per line (`ts, level, service, node, correlation_id, msg, …`). The `node` is the pod name. Easy to `grep`/`jq`, and ready for a log stack later.
- **One error format** (`errors.py`): `{"error": {"code", "message", "correlation_id"}}`, with 400 invalid, 404 missing, 409 conflict/invalid transition, 503 temporarily unavailable (with `Retry-After`), 504 outcome unknown.
- **Retry with exponential backoff and jitter** (`retry.py`, ADR-16): 0.2 s doubling up to 2 s, 5 retries, randomised. It exists so that when storage is briefly down, callers don't all hammer it in lockstep (a retry storm). **Only "not applied" errors are retried** (connection refused, 503); a timeout after sending is *unknown*, so it surfaces as 504 and the caller decides.
- **Config only from environment variables** (`config.py`): a ConfigMap in Kubernetes, `environment:` in compose.

---

## 8. Docker and Kubernetes

- **One Dockerfile**, built once per service with `--build-arg SERVICE=<name>`. The base layer (Python + dependencies installed by `uv` from the lockfile) is shared, so builds are fast; each service still ships and scales as its own image. Runs as an unprivileged user.
- **docker-compose** (`docker-compose.yml`, plus an nginx container standing in for the Ingress): the same topology on plain Docker, for development.
- **Kubernetes** (`k8s/`, namespace `erm`, 14 pods):

| Need | Object | Why |
|---|---|---|
| storage nodes | one **StatefulSet per shard** (2 pods), headless Service, `volumeClaimTemplates` | stable names (`shard-1-0`), stable DNS, and a volume that survives restarts: this is what makes *delta* resync possible |
| stateless services | **Deployments** (2 pods each) + ClusterIP Services | any pod is interchangeable; K8s load-balances and replaces dead ones |
| coordinator | Deployment, `replicas: 1`, `strategy: Recreate` | never two at once, even during an update |
| RabbitMQ | StatefulSet (1) | spec: one instance |
| public entry | **Ingress** (paths `/`, `/requests`, `/resources`, `/match`) | API gateway; `/internal/*` is **not** routed, so internal APIs stay inside the cluster |
| config / secrets | ConfigMap, Secret (RabbitMQ credentials) | no config in images |
| health | liveness `/health`, readiness `/ready` per pod | restart hung pods; don't send traffic to unready ones |
| autoscaling | **HPA** on matching-service: 2–5 pods at 50% CPU | stateless competing consumers: more pods = more throughput |

Self-healing is two layers: **Kubernetes** replaces dead pods; our **application logic** (failover, resync, saga retries) makes the replacement seamless.

---

## 9. The frontend

One self-contained page (`services/request_service/static/index.html`, ~750 lines) served by request-service at `/`. No framework, no extra pod. It uses **only the public API** (same endpoints as curl).

- **Tabs:** *I need help* (what / how many / how urgent / where, with a live hint "3 helpers nearby can cover this" computed in the browser), *I can help* (register an offer), *Live board* (counters, activity feed, map, demo tools), *System* (the coordinator's dashboard embedded in an iframe on port 8090).
- **The tracker:** polls the person's own request every 1.2 s and the lists every 3 s; shows a 4-step progress bar, the helper's name and distance, and a dashed line on the map. Buttons map to the lifecycle: "Help has arrived" → FULFILLED, "Cancel" → CANCELLED.
- **It applies the same distributed-systems rules as the backend:** on a 503 it retries automatically and shows "The system is recovering from a failure. Retrying…"; a *dropped connection* is retried only for GETs, **never for POSTs** (the POST may have gone through, so retrying could create a duplicate request). Older poll responses can't overwrite newer state.

---

## 10. "What happens when X dies?"

| Failure | What happens | Client impact |
|---|---|---|
| a **replica** storage pod | next write to its shard fails to replicate → node marked STALE; writes continue (degraded, one copy). On restart it resyncs the delta | none |
| a **primary** storage pod | the next request gets "connection refused" → immediate failover to the IN_SYNC replica, retried once. Dead node resyncs on return | none (maybe a few ms) |
| a write **in flight** during failover | acknowledged only if the new primary has it; otherwise 503 and the client retries | a retry |
| **both** nodes of one shard | that shard answers 503; other shards keep working; list queries return `partial: true`. The last primary is awaited (or elected after a coordinator restart) | requests for that shard's keys fail until recovery |
| the **coordinator** | all storage calls answer 503 for ~5–7 s while Kubernetes restarts it and nodes re-register; services retry with backoff | brief 503s with `Retry-After` |
| a **request/resource-service** pod | the other pod serves; Kubernetes recreates it | maybe one 502 from the Ingress on an in-flight request |
| a **matching** pod mid-saga | its unacked message is redelivered to another pod, whose saga resumes from the reservation held | none (delay) |
| **RabbitMQ** | requests are still accepted (`queued_for_matching: false`, answered within ~2 s); consumers reconnect on their own; the reconciler re-sends orphaned requests | none |
| a **poison message** | rejected → dead-letter queue (visible on the dashboard) | none |
| slow network | timeouts become 503/504; idempotent retries; no failover of a merely slow node | slower |

---

## 11. The bugs the chaos test found (and why that matters)

`make chaos` runs traffic while randomly killing storage pods, then checks six guarantees (no acknowledged request lost, no acknowledged cancellation reverted, no over-allocation, depots and requests agree, nothing stuck PENDING, replicas identical). Running it on the heavier Tier 2 load exposed real bugs. Several were already latent on `main`. Each has a unit test that fails on the old code.

| # | Bug | Why it was wrong | Fix |
|---|---|---|---|
| 1 | Finishing a resync set `IN_SYNC` unconditionally | overwrote a STALE set meanwhile → a replica that silently lacked writes | only a SYNCING node can become IN_SYNC |
| 2 | A STALE replica could win the election | the returning primary was then resynced *from the stale node*, deleting acknowledged writes | the shard waits for its last primary |
| 3 | A write racing a failover | applied on the old primary, skipped the *new* primary, but was acknowledged | replicate to every node but the writer; 503 unless the current primary has it |
| 4 | Failover on a mere read timeout | a slow node was declared dead → resyncs → more load | only refused/reset connections fail over; read repair for timed-out writes |
| 5 | The saga released quantity after a timed-out assign | the assign had succeeded → request ASSIGNED with no quantity held | retry idempotent steps, re-read on conflict, compensate only on definite failure |
| 6 | Wake-up re-matched the same 50 requests on every event | ~37 wasted writes each | wake only as many as the freed units can serve, coalesce events |
| 7 | Cancel of a never-assigned request, and 504s, were dead-lettered | lost work | ignore / requeue |
| 8 | Publishing hung while RabbitMQ was down | the HTTP request hung until the Ingress gave up | 2 s publish timeout |
| 9 | Server keep-alive (5 s) shorter than clients' | 502s and false failovers | 75 s |

**How to present this:** "we didn't just claim it was fault tolerant, we built a test that tries to break it, and it broke it nine times; here is what each one taught us." Honest, concrete, and exactly what an examiner wants to hear.

---

## 12. Patterns and principles: where each lives

| Pattern / principle | Where |
|---|---|
| API Gateway | Ingress / nginx path routing |
| Microservices, database-per-service (logical) | the 3 services and their key prefixes |
| Hexagonal / ports & adapters | `domain.py` (pure) ← `service.py` ← `main.py`; adapters: `storage_client.py`, `amqp.py`, `clients.py` |
| Repository | `StorageClient` |
| Strategy | `strategies.py` (`nearest`, `balanced`), `Partitioner` |
| State | request lifecycle table |
| Publish–subscribe, competing consumers | `erm.events`, `matching.q` |
| Saga / compensating transaction | `saga.py`, cancel → release |
| Optimistic concurrency control | versions, `If-Match`, 409 |
| Idempotent consumer / idempotent operations | reserve, release, matching skip, `StorageClient.create` |
| Retry with backoff + jitter, dead-letter channel | `retry.py`, `erm.retry`, `erm.dlx` |
| Leader–follower replication, heartbeat, anti-entropy | coordinator + storage nodes |
| Reconciliation loop | the PENDING reconciler, anti-entropy |
| Correlation identifier, health check | logging middleware, probes |
| SOLID | SRP (one job per service/module), OCP (new strategy = new class), DIP (domain depends on ports) |

**CAP in one sentence:** when a shard can't guarantee its copies are consistent, it answers 503 instead of risking wrong data (CP), and the rest of the system keeps working.

---

## 13. Known limitations (say them before the examiner does)

- **One coordinator** (required by the spec) is a single point of failure for ~5–7 s per restart, and a throughput bottleneck: under heavy chaos load it saturates one CPU core. A fix needs a consensus protocol such as Raft, or partitioned coordinators.
- **The restart election uses `version_sum`**, a heuristic, not a real replicated log position.
- **If a shard's last primary loses its disk for good**, the shard stays unavailable until an operator steps in (deliberate CP).
- **A `request.cancelled` event lost while RabbitMQ is down** isn't repaired automatically; the units stay reserved.
- **A hot resource** (many requests for the same record) is processed serially, about 30 per second: correct, but slower. Load spread over several resources scales with pods.
- **No authentication** (out of scope; stated in the design).
- **Map tiles and library come from the internet**; without it the page shows a notice and everything else works.

---

## 14. Viva questions, short answers

1. **Why 12 partitions, not `mod 3`?** Adding a shard moves 25% of keys instead of ~75%. A test proves it.
2. **Why synchronous replication?** An acknowledged write is on two nodes, so failover can't lose it. Cost: latency and degraded mode.
3. **How do you avoid split brain?** Nodes never promote themselves; only the single coordinator assigns roles.
4. **What happens to writes the old primary had but never replicated?** They were never acknowledged; resync overwrites them ("primary wins").
5. **Two matchers want the last units. What happens?** Compare-and-set: one write wins, the other gets 409, re-reads and sees fewer units. No locks.
6. **Why a saga, not a transaction?** Two services, two shards. Reserve, then assign, with release as the compensation.
7. **A message is delivered twice. Is that a problem?** No: every handler is idempotent.
8. **What if a call times out mid-saga?** We don't guess. Idempotent steps are retried, conflicts are resolved by re-reading, compensation happens only on definite failure, and redelivery resumes.
9. **How do you detect a dead node?** Heartbeats every 2 s (SUSPECT at 4 s, DEAD at 6 s), and instantly when a request is refused. Slow ≠ dead: a timeout does not trigger failover.
10. **Why StatefulSets for storage?** Stable names and volumes: a restarted node keeps its data and only copies the delta.
11. **Why not shard by location?** A disaster is local; one shard would take all the load. Matching scatter-gathers instead.
12. **What stops retry storms?** Exponential backoff with jitter, `Retry-After` on 503, and retries at priority 0 so fresh work goes first.
13. **How do you know it's correct?** The chaos test checks six guarantees automatically under random pod kills. It found nine bugs; each now has a regression test.
14. **What's the weakest part?** The single coordinator: a bottleneck and a short outage on restart. Mitigated by soft state, retries and fast restart.
15. **What would you do with more time?** Replace the single coordinator with a replicated one (Raft), add a persistent replication log instead of `version_sum`, and add authentication.

---

## 15. Suggested reading order (about 3 hours)

1. `DESIGN.md` §1–4: goals, requirements, architecture.
2. `services/coordinator/cluster.py`: all failover rules in one pure file. Then `tests/unit/test_storage.py` to see them exercised.
3. `services/storage_node/store.py` + `services/coordinator/router.py`: versions, the write path, resync.
4. `services/request_service/domain.py` and `services/resource_service/domain.py`: small pure files.
5. `services/matching_service/saga.py` + `tests/unit/test_saga.py`: the delicate part.
6. `services/common/amqp.py` and `k8s/`: the topology and the deployment.
7. Run `scripts/demo/06-storage-failure.sh` with the dashboard open, then `make chaos`, and watch the events.
8. `DESIGN.md` §7 (ADRs) and §10 (what changed after the freeze, and the bugs).

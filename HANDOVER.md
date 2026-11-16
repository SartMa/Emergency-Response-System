# Handover: Phase A → Phase B

Written 2026-11-16 at the end of Phase A. Deadline **2026-11-18**.
Read this first, then `DESIGN.md` (the why) and `CLAUDE.md` (the rules + commands).

## 1. Where things stand

**Everything mandatory in the spec is built and verified, on docker-compose and on minikube.**

| Spec item | Status | Proof |
|---|---|---|
| 5 operations + REST API (Table 2) with JSON and correct codes | done | `tests/integration`, demos 01–03 |
| Coordinator + 3 shards × 2 nodes, hash routing | done | demo 04, dashboard |
| Heartbeats, failure detection, failover to replica | done | demo 06; a primary was killed during 100 async requests: 0 errors, 0 lost writes |
| Resync of a recovered node | done | demo 06: "resynced (N keys copied)", digests identical |
| RabbitMQ asynchronous request → matching | done | demo 05 |
| Docker images + docker-compose | done | `make up` |
| Kubernetes: Deployments, StatefulSets, Services, Ingress, probes, ConfigMap/Secret | done | `make k8s-deploy`, all 14 pods ready |
| K8s pod failure + recovery, scaling | done | demos 08, 09 |
| Tests: normal + failure | done | 26 tests pass on compose; the 7 integration tests also pass on minikube |

**Our extras (Tier 1), all done:** geo matching with quantity, no over-allocation under concurrency (optimistic concurrency), saga with compensation on cancel, priority queue by urgency, DLQ, live dashboard, `/internal/locate`, status history, correlation IDs in JSON logs, coordinator self-rebuild after restart, and the `balanced` strategy (from Tier 2).

## 2. Run it

```bash
# local
make up                     # API http://localhost:8080, dashboard http://localhost:8090/dashboard
make test                   # 26 tests
make down                   # stops and wipes volumes

# kubernetes (minikube is already set up on Sarthak's laptop)
make k8s-start              # only if minikube is stopped
make k8s-build              # ~2 min: builds 5 images, loads them into minikube
make k8s-deploy             # applies k8s/ and waits for readiness
make k8s-gateway            # terminal 1: Ingress on :8080
make k8s-dashboard          # terminal 2: dashboard on :8090
```

Don't run compose and minikube port-forwards at the same time: both use ports 8080 and 8090.

## 3. Demo runbook (spec §10, in this order)

Keep `http://localhost:8090/dashboard` visible next to the terminal the whole time.

| # | Script | Spec item | What to point at |
|---|---|---|---|
| 1 | `01-create-get.sh` | Request creation | 201, `queued_for_matching: true`, ETag = version, 400/404 |
| 2 | `02-register.sh` | Resource registration | `partial: false` = all 3 shards answered the scatter-gather |
| 3 | `03-match.sh` | Matching | closest resource *with enough quantity* wins; cancel gives quantity back (saga) |
| 4 | `04-distribution.sh` | Data distribution | partition → shard per key; key counts per node on the dashboard |
| 5 | `05-async-burst.sh` | Async processing | 100 requests, 60 units, **exactly 60 assigned**; per-pod message counts |
| 6 | `06-storage-failure.sh` | Storage failure + node recovery | before/during/after; event log: failover, then "resynced (N keys copied)" |
| 7 | `08-k8s-pod-recovery.sh` | K8s recovery | 40/40 requests succeed while a pod is deleted; coordinator rebuilds itself |
| 8 | `09-k8s-scale.sh` | K8s scaling | 4 pods share the queue roughly evenly |

Demo 08 restarts the coordinator, so restart `make k8s-dashboard` after it.
For a longer "during" window in demo 06 on Kubernetes, scale the StatefulSet instead of deleting a pod:
`kubectl -n erm scale sts/shard-0 --replicas=1` (removes `shard-0-1`), then `--replicas=2`.

## 4. What's left (Phase B)

In priority order. Each item lists where to change the code.

1. **Practise the demo** on minikube end to end (30 min). Everything is scripted.
2. **Report / slides.** `DESIGN.md` §2–7 + §10 have nearly everything: requirements, diagram, ADR table, patterns table. Add screenshots of the dashboard during demo 06.
3. **Tier 2 features** (optional; each is small, and the hooks are already there):
   - *Retry with backoff* (FR-19): in `common/amqp.py` declare `erm.retry` + `matching.retry.q` with `x-dead-letter-exchange=erm.commands`. In `matching_service/saga.py`, where it says "No resource", republish with a per-message `expiration` instead of marking UNMATCHED until 5 attempts.
   - *Wake-up on new supply*: resource-service already publishes `resource.registered`. Bind a queue to it in `declare_topology`, consume in matching-service, and re-match `GET /requests?status=UNMATCHED&type=X`.
   - *Reconciler* (DESIGN.md §6.2): a background task in request-service that re-publishes requests that have been PENDING for more than 30 s (`service.list("PENDING", None)` + `publish_match`).
   - *Queue depths on the dashboard*: the coordinator can call the RabbitMQ management API (`/api/queues`).
   - *HPA*: `minikube addons enable metrics-server`, then `kubectl autoscale deployment matching-service --min=2 --max=5 --cpu-percent=50`.
4. Run `make test` after any change. If you touch cluster logic, also run demo 06.

## 5. Known limitations (say them before the examiner does)

- **Single coordinator**: if its pod dies, storage returns 503 for about 5–7 s until Kubernetes restarts it and nodes re-register. Required by the spec (1 coordinator). Mitigated by soft state; a fix would need consensus (DDIA ch. 10).
- **Hot resource**: many requests for the same resource are processed one after another (correct but slower: about 30 per second). Spread-out load scales with pods.
- **Events lost if RabbitMQ is down**: requests are still stored (201, `queued_for_matching: false`) but stay PENDING until someone calls `POST /match/{id}`, or until the Tier 2 reconciler exists.
- **Degraded mode**: if a replica is down, writes are acknowledged with one copy. If the primary then also dies before the replica resyncs, the shard becomes unavailable (it does not lose data, it waits).
- **Election after a coordinator restart** uses `version_sum` as a "who has seen more writes" heuristic, not a real log position.
- No authentication (out of scope; stated in NFR-12).

## 6. Likely viva questions (short answers; details in DESIGN.md)

- **Why 12 partitions and not `mod 3`?** Going to 4 shards would move about 75% of keys with mod-N, against 25% with fixed partitions. `tests/unit/test_domain.py::test_fixed_partitions_move_fewer_keys_than_mod_n` proves it.
- **Why synchronous replication?** A write is acknowledged only once it is on both nodes, so failover never loses an acknowledged write. The cost is latency, and degraded mode when the replica is down.
- **How do you avoid split brain?** Nodes never promote themselves. Only the single coordinator assigns roles.
- **What happens to writes the old primary had but never replicated?** They were never acknowledged, so on resync the node is made identical to the new primary ("primary wins"), and those writes are discarded (DDIA ch. 6).
- **How do you prevent two matchers taking the same units?** Compare-and-set on a per-key version. The loser gets 409, re-reads and re-checks quantity. No locks.
- **Why a saga and not a transaction?** The request and resource are on different shards and owned by different services. Reserve → assign, with release as compensation if assign fails.
- **At-least-once delivery: what if a message is processed twice?** Matching skips requests that aren't PENDING/UNMATCHED, and reserve is idempotent per `request_id`.
- **Why not shard by location?** A disaster is local, so one shard would take all the load (ADR-15).
- **Why StatefulSets for storage?** Stable names (`shard-1-0`), stable DNS, and a volume that survives restarts, which is what makes delta resync possible.
- **How is failure detected?** Heartbeat every 2 s (SUSPECT after 4 s, DEAD after 6 s), and immediately when a request to the primary fails (fast failover).

# Handover: Phase A → Phase B

Written 2026-11-16 at the end of Phase A. Deadline **2026-11-20** (moved from 11-18).
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
| Tests: normal + failure | done | 50 tests pass (42 unit, 8 integration) on minikube, plus `make chaos` (6 automatic guarantee checks under random pod kills) |

**Our extras (Tier 1), all done:** geo matching with quantity, no over-allocation under concurrency (optimistic concurrency), saga with compensation on cancel, priority queue by urgency, DLQ, live dashboard, `/internal/locate`, status history, correlation IDs in JSON logs, coordinator self-rebuild after restart, and the `balanced` strategy (from Tier 2).

## 2. Run it

```bash
# local
make up                     # API http://localhost:8080, dashboard http://localhost:8090/dashboard
make test                   # 50 tests
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
| 9 | `make chaos` (beyond the spec) | Proof under repeated failure | 2 min of random kills, then 6 checks print PASS; watch "copies identical" on the dashboard return after each kill |

Start the demo with `00-seed-mumbai.sh` and the landing page at http://localhost:8080 (live map, needs internet). Creating a request from the page makes the map fly to it; a few seconds later its dot turns green with a dashed line to the resource serving it.

Demo 08 restarts the coordinator, so restart `make k8s-dashboard` after it.
For a longer "during" window in demo 06 on Kubernetes, scale the StatefulSet instead of deleting a pod:
`kubectl -n erm scale sts/shard-0 --replicas=1` (removes `shard-0-1`), then `--replicas=2`.

### If the API stops answering on minikube
Symptom: requests through :8080 hang, or pods can't reach `coordinator` / `request-service` by name, right after pods were restarted. Cause seen on 2026-11-16: minikube's own system pods (kube-proxy, ingress controller) had stale API tokens (`Unauthorized` in their logs), so they kept routing to old pod IPs. It is not an application bug. Fix:
```bash
kubectl -n kube-system delete pod -l k8s-app=kube-proxy
kubectl -n ingress-nginx rollout restart deployment/ingress-nginx-controller
# then restart `make k8s-gateway` and `make k8s-dashboard`
```
Before the real demo, do a fresh `minikube stop && minikube start`, and don't let the laptop sleep in between.

## 4. What's left (Phase B)

In priority order. Each item lists where to change the code.

1. **Practise the demo** on minikube end to end (30 min). Everything is scripted.
2. **Report / slides.** `DESIGN.md` §2–7 + §10 have nearly everything: requirements, diagram, ADR table, patterns table. Add screenshots of the dashboard during demo 06.
3. **Tier 2 features: done, tested and merged into `main` (2026-11-17).** See §4.1.
4. Run `make test` after any change. If you touch cluster logic, also run `make chaos`.

Ownership changed on 2026-11-17: Sarthak owns this project (ERM) to the end. The friend does CSE 816 separately.

### 4.1 Tier 2 (formerly branch `feature/tier2`): what's in it and how it was tested

**Status: tested on minikube and merged.** The checklist below was run in full on 2026-11-17. The chaos test found several real bugs along the way, some of them already in `main` (see DESIGN.md §10 N12). All are fixed. Final results: 50 tests pass; 3 normal chaos runs in a row and 1 aggressive run had every check passing and every write accepted. Checklist results: retry chain 2/4/8/16/32 s then stop (traced for one request); wake-up within ~1 s of new supply; reconciler re-sent a request created while RabbitMQ was down, and it was matched 19 s after RabbitMQ came back; queue table shows all 9 queues; HPA scaled 2 → 5 pods at 202% CPU.

The checklist is kept below for re-testing after future changes.

| Feature | Where | What it does |
|---|---|---|
| Retry with backoff (FR-19) | `common/amqp.py` (topology), `matching_service/main.py`, `matching_service/policies.py` | No resource → request becomes UNMATCHED at once and is retried after 2, 4, 8, 16, 32 s (one TTL queue per delay, `matching.retry.<N>s`), then stops |
| Wake-up on new supply | `resource_service/service.py` publishes `resource.registered/restocked/released` with lat/lon; `matching_service/main.py` consumes `matching.wakeup.q` | Nearby UNMATCHED requests of that type get one more attempt, most urgent first, max 50 per event |
| Reconciler | `request_service/main.py` (loop every ~15 s), `service.reconcile_pending`, `domain.stale_pending` | Re-sends requests still PENDING after 30 s (at most once a minute per pod) |
| Queue depths on dashboard | `coordinator/main.py` `GET /internal/queues`, `static/dashboard.html`; RabbitMQ creds added to `k8s/20-coordinator.yaml` and compose | Table of waiting / in-flight / consumers / rates per queue |
| HPA | `k8s/60-hpa.yaml`, `make k8s-start` enables metrics-server, `scripts/demo/10-k8s-autoscale.sh` | matching-service 2..5 pods on CPU |

**Test checklist (in this order):**
1. `git switch feature/tier2`, then `make unit`, which should show 26 passing.
2. **Rebuild everything**, because 4 of the 5 images changed: `make k8s-build && kubectl apply -f k8s/` and restart all deployments, or for local `make down && make up`. With compose, `make down` wipes data, which is fine.
3. `make test`. The integration tests include a new one, `test_wakeup_on_new_and_freed_supply`, plus a modified `test_sync_match_endpoint_and_cancel_compensation` (wake-up and `POST /match` now race, and the test accepts either winner).
4. **Retry**: create a request with no resource nearby, e.g. `MODE=k8s ./scripts/demo/03-match.sh`. In `kubectl -n erm logs -l app=matching-service --tail=-1 -f` you should see `retry 1/5 in 2s`, `2/5 in 4s` and so on, then `retries used up`. On the dashboard, the `matching.retry.*` queues briefly show 1 waiting message.
5. **Wake-up**: the last step of demo 03 registers a shelter and the waiting request should become ASSIGNED within a second or two without calling `/match`. Logs show `wake-up: 1 waiting shelter request(s) re-queued`.
6. **Reconciler**: stop RabbitMQ (`kubectl -n erm scale sts/rabbitmq --replicas=0`), create a request (201 with `queued_for_matching: false`), start RabbitMQ again (`--replicas=1`). Within ~45 s request-service logs `reconciler: re-sent 1 request(s)` and the request gets matched.
7. **Queue depths**: open the dashboard (`make k8s-dashboard`); the "Message queues" table should list 9 queues with consumers > 0 on `matching.q`, `matching.wakeup.q` and `resource.release.q`. Run demo 05 and watch `matching.q` fill and drain.
8. **HPA**: `minikube addons enable metrics-server`, wait ~1 min, `kubectl -n erm get hpa` should show a CPU percentage (not `<unknown>`). Then `./scripts/demo/10-k8s-autoscale.sh`.
9. `make chaos` should still print `ALL GUARANTEES HELD`. This matters most: wake-up and retries add a lot of background matching while nodes die.
10. If all pass, merge: `git switch main && git merge feature/tier2`.

**Things to watch for:**
- RabbitMQ refuses to redeclare an existing queue with different arguments. The new queues have new names, so this shouldn't happen, but if a service logs `PRECONDITION_FAILED`, delete the stale queue in the RabbitMQ UI (or reset with `make k8s-delete`).
- Demo 09 scales matching manually. With the HPA applied, the HPA may change the replica count back afterwards. That's expected.
- Every UNMATCHED request is now retried 5 times over about a minute, so the logs are busier than before. That's normal.

## 5. Known limitations (say them before the examiner does)

- **Single coordinator**: if its pod dies, storage is unavailable for about 5–7 s until Kubernetes restarts it and nodes re-register. Services ride most of that out with exponential backoff (ADR-16); a few requests may still get 503 with `Retry-After`. Required by the spec (1 coordinator). Mitigated by soft state; a fix would need consensus (DDIA ch. 10).
- **Hot resource**: many requests for the same resource are processed one after another (correct but slower: about 30 per second). Spread-out load scales with pods.
- **RabbitMQ down**: requests are still stored (201 within ~2 s, `queued_for_matching: false`) and the reconciler re-sends them within about 45 s of RabbitMQ returning. A `request.cancelled` event lost while RabbitMQ is down is not repaired automatically: the quantity stays reserved until the matching saga next sees that request.
- **Coordinator CPU**: every storage operation goes through one Python process. Under chaos load with many retries it saturates about 1 core and latency rises from ~25 ms to ~1 s. Correctness holds. Scaling it out would need partitioned coordinators or consensus.
- **Waiting for a lost primary**: if a primary dies while its replica is not in sync, the shard waits for that node to come back, because it holds every acknowledged write. If its disk were lost for good, the shard would stay unavailable until an operator steps in (a deliberate CP choice).
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
- **How do you know it's correct, not just that the demo works?** The chaos test kills random storage pods under load and checks six guarantees automatically. It found real bugs: a resync race, an unsafe election, a write racing a failover, and a saga that guessed on timeouts. Each one now has a unit test that fails on the old code (DESIGN.md §10 N12).
- **What does the saga do when a call times out?** It doesn't guess. Every step is idempotent, so it retries; on a conflict it re-reads (assigned to this resource = our earlier attempt succeeded); it compensates only on a definite failure; and a redelivered message resumes from the reservation already held.
- **What stops a retry storm during failover?** Callers back off exponentially with jitter (0.2 s doubling to 2 s), and 503s carry `Retry-After` (ADR-16).
- **How is failure detected?** Heartbeat every 2 s (SUSPECT after 4 s, DEAD after 6 s), and immediately when a request to the primary fails (fast failover).

# Developer guide

How to work on this codebase: where things are, the rules the code follows, and the commands you'll need.

## Documents
- **`DESIGN.md`**: requirements, architecture and design decisions (ADRs). §10 lists the implementation notes added after the design freeze. Read it before changing anything.
- **`HOW_IT_WORKS.md`**: the running system step by step: one request end to end, the storage tier, the saga, RabbitMQ, Kubernetes, failure scenarios, and the bugs found by chaos testing.
- **`HANDOVER.md`**: current status, how to run, demo runbook.
- The design is **frozen** (2026-11-16). Changes go through an ADR update in `DESIGN.md`.
- Keep code readable. Comments explain *why* and point to the decision (e.g. `# ADR-06: optimistic concurrency`).

## Stack
Python 3.12, FastAPI, uvicorn, httpx (async), aio-pika, pydantic v2, SQLite (stdlib), pytest.
Dependencies are managed with **uv** (one project at the repo root + `uv.lock`); use `uv add` / `uv run`, never pip. Docker images install with uv too.
Docker (`python:3.12-slim`), docker-compose for local development, minikube for Kubernetes (namespace `erm`).

## Layout
```
services/common/           shared: config, JSON logging + correlation ID, errors, partitioner, geo, AMQP, storage client
services/storage_node/     store.py (SQLite KV with versions, tombstones, digest) + main.py (API, heartbeat, /internal/sync)
services/coordinator/      cluster.py (PURE failover/election rules) + router.py (routing, replication, resync) + main.py + static/dashboard.html
services/request_service/  domain.py (state machine) -> service.py (CAS updates, publishing) -> main.py (API) + static/index.html (frontend)
services/resource_service/ domain.py (quantity, reserve/release) -> service.py -> main.py (API + cancel consumer)
services/matching_service/ strategies.py (Strategy pattern) + saga.py (reserve -> assign -> compensate) + policies.py + clients.py + main.py
k8s/                       manifests (apply in filename order)
deploy/gateway/nginx.conf  local stand-in for the Ingress
scripts/demo/              demo scripts, one per spec §10 item (MODE=k8s|compose)
scripts/chaos.py           chaos test: random pod kills + 6 automatic guarantee checks
scripts/ui_e2e.py          real-browser test of the frontend
tests/unit, tests/integration
```

## Rules for code
- Hexagonal per service: API (`main.py`) → application service (`service.py`) → pure domain logic (`domain.py`, no I/O, unit-tested); adapters for storage, HTTP and AMQP.
- Services never talk to storage nodes directly. Everything goes through the coordinator. Matching never writes storage itself; it calls the owning service.
- Every read-modify-write uses versions (`expected_version` / `If-Match`). Never blindly overwrite a request or resource.
- Hash with `hashlib.sha1`, never the built-in `hash()`, which is randomised per process.
- Propagate `X-Correlation-ID` on every HTTP call and AMQP message. Log JSON with `service`, `node`, `correlation_id`.
- Config only from environment variables (a ConfigMap in Kubernetes). No hard-coded hostnames.
- Consumers: manual ack, idempotent; 503/504 → requeue; any other unexpected error → `nack(requeue=False)` (dead-letter queue).
- Error body: `{"error": {"code": "...", "message": "...", "correlation_id": "..."}}`. Codes: 400 invalid, 404 missing, 409 conflict / invalid transition, 503 temporarily unavailable (with `Retry-After`), 504 outcome unknown.

## Commands
`make help` lists everything. The common ones:
- `make unit`: unit tests, no Docker needed
- `make up` / `make down`: local stack (API and frontend on :8080, dashboard http://localhost:8090/dashboard, RabbitMQ UI :15672)
- `make test`: unit + integration tests (needs the stack running, locally or via the Kubernetes port-forwards)
- `make chaos`: 2 minutes of traffic with random storage-pod kills, then 6 guarantee checks
- `make k8s-start`, `make k8s-build`, `make k8s-deploy`: minikube from scratch
- `make k8s-gateway` and `make k8s-dashboard`: port-forwards to :8080 and :8090 (keep them running)
- `make k8s-delete`: remove everything from the cluster, including all stored data
- `MODE=k8s ./scripts/demo/06-storage-failure.sh` (or `MODE=compose`): demos; `NO_PAUSE=1` skips the prompts

## Gotchas
- After changing code for Kubernetes: `make k8s-build`, then `kubectl -n erm rollout restart deployment/<name>` (or `statefulset/shard-N`).
- The coordinator must stay a single process (its cluster map is in memory). Never add uvicorn workers or replicas to it.
- `kubectl logs -l ...` shows only 10 lines per pod unless you pass `--tail=-1`.
- The dashboard is on port 8090 because 8081 was taken on the development machine.
- minikube's hostpath provisioner stores a volume in `/tmp/hostpath-provisioner/<namespace>/<claim name>` on the node and does NOT delete it when the namespace is deleted. A redeploy then reuses the old folders and comes back with all the old data. `make k8s-delete` removes them explicitly; don't replace it with a bare `kubectl delete namespace erm`.
- Don't run `pkill -f port-forward` (or similar) from a shell command whose own text contains that pattern: it kills the shell itself. Find the PID with `ps` and `kill <pid>`.
- Port-forwards to `svc/coordinator` die whenever the coordinator pod restarts; reopen with `make k8s-dashboard`.
- If the API stops answering on minikube after pods restarted (stale tokens in kube-proxy or the ingress controller), see "If the API stops answering" in `HANDOVER.md`.

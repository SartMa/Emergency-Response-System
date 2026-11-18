# CLAUDE.md

Semester 7 Cloud Computing project: **Distributed Emergency Resource Matching System**.
Spec: `Emergency Response Matching-1.pdf`. Deadline: **2026-11-20**.

## Source of truth
- **`DESIGN.md`** holds the requirements, architecture and ADRs (§10 lists post-freeze implementation notes). Read it before changing anything.
- **`HANDOVER.md`** holds the current status, demo runbook and the remaining work.
- **`HOW_IT_WORKS.md`** explains the running system step by step: one request end to end, the storage tier, the saga, RabbitMQ, Kubernetes, failure scenarios, the chaos-found bugs and viva answers.
- Design status: **FROZEN** (2026-11-16).
- After the freeze, don't deviate from DESIGN.md silently. If something has to change, ask the user first, then update the relevant ADR.
- The students must defend every choice in a viva. Keep code readable. Comments explain *why* and point to the decision (e.g. `# ADR-06: optimistic concurrency`). Avoid clever code.

## Stack
Python 3.12, FastAPI, uvicorn, httpx (async), aio-pika, pydantic v2, SQLite (stdlib), pytest.
Dependencies are managed with **uv** (one project at the repo root + `uv.lock`); use `uv add` / `uv run`, never pip. Docker images install with uv too.
Docker (`python:3.12-slim`), docker-compose for local development, minikube for Kubernetes (namespace `erm`).

## Layout
```
services/common/           shared: config, JSON logging + correlation ID, errors, partitioner, geo, AMQP, storage client
services/storage_node/     store.py (SQLite KV with versions, tombstones, digest) + main.py (API, heartbeat, /internal/sync)
services/coordinator/      cluster.py (PURE failover/election rules) + router.py (routing, replication, resync) + main.py + static/dashboard.html
services/request_service/  domain.py (state machine) -> service.py (CAS updates, publishing) -> main.py (API)
services/resource_service/ domain.py (quantity, reserve/release) -> service.py -> main.py (API + cancel consumer)
services/matching_service/ strategies.py (Strategy pattern) + saga.py (reserve -> assign -> compensate) + clients.py + main.py
k8s/                       manifests (apply in filename order)
deploy/gateway/nginx.conf  local stand-in for the Ingress
scripts/demo/              demo scripts, one per spec §10 item (MODE=k8s|compose)
tests/unit, tests/integration
```

## Rules for code
- Hexagonal per service: `api/` → `domain/` (pure, no I/O, unit-tested) → `adapters/`.
- Services never talk to storage nodes directly. Everything goes through the coordinator. Matching never writes storage itself; it calls the owning service.
- Every read-modify-write uses versions (`expected_version` / `If-Match`). Never do a blind overwrite of a request or resource.
- Hash with `hashlib.sha1`, never the built-in `hash()`, which is randomised per process.
- Propagate `X-Correlation-ID` on every HTTP call and AMQP message. Log JSON with `service`, `node`, `correlation_id`.
- Config only from env vars (ConfigMap in K8s). No hard-coded hostnames.
- Consumers: manual ack, idempotent, `nack(requeue=False)` on unexpected errors (→ DLQ).
- Error body: `{"error": {"code": "...", "message": "...", "correlation_id": "..."}}`. Codes: 400 invalid, 404 missing, 409 conflict / invalid transition, 503 storage or broker unavailable.

## Commands
`make help` lists everything. The common ones:
- `make unit`: unit tests, no Docker needed
- `make up` / `make down`: local stack (API :8080, dashboard http://localhost:8090/dashboard, RabbitMQ UI :15672)
- `make test`: unit + integration tests (needs the stack running, locally or via k8s port-forwards)
- `make k8s-start`, `make k8s-build`, `make k8s-deploy`: minikube from scratch
- `make k8s-gateway` and `make k8s-dashboard`: port-forwards to :8080 and :8090 (keep them running)
- `MODE=k8s ./scripts/demo/06-storage-failure.sh` (or `MODE=compose`): demos; `NO_PAUSE=1` skips the prompts

Gotchas:
- After changing code for Kubernetes: `make k8s-build`, then `kubectl -n erm rollout restart deployment/<name>` (or `statefulset/shard-N`).
- The coordinator must stay a single process (its cluster map is in memory). Never add uvicorn workers or replicas to it.
- `kubectl logs -l ...` shows only 10 lines per pod unless you pass `--tail=-1`.
- Port 8081 is used by another app on Sarthak's machine, which is why the dashboard uses 8090.
- minikube's hostpath provisioner stores a volume in `/tmp/hostpath-provisioner/<namespace>/<claim name>` on the node and does NOT delete it when the namespace is deleted. A redeploy then reuses the old folders and comes back with all the old data. `make k8s-delete` removes them explicitly; don't replace it with a bare `kubectl delete namespace erm`.
- Never run `pkill -f port-forward` (or similar) from a shell command whose own text contains that pattern: it kills the shell itself. Find the PID with `ps` and `kill <pid>`.
- Port-forwards to `svc/coordinator` die whenever the coordinator pod restarts; reopen with `make k8s-dashboard`.

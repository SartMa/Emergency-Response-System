# CLAUDE.md

Semester 7 Cloud Computing project: **Distributed Emergency Resource Matching System**.
Spec: `Emergency Response Matching-1.pdf`. Deadline: **2026-11-18**.

## Source of truth
- **`DESIGN.md`** holds the requirements, architecture and ADRs. Read it before changing anything.
- Design status: **FROZEN** (2026-11-16).
- After the freeze, don't deviate from DESIGN.md silently. If something has to change, ask the user first, then update the relevant ADR.
- The students must defend every choice in a viva. Keep code readable. Comments explain *why* and point to the decision (e.g. `# ADR-06: optimistic concurrency`). Avoid clever code.

## Stack
Python 3.12, FastAPI, uvicorn, httpx (async), aio-pika, pydantic v2, SQLite (stdlib), pytest.
Dependencies are managed with **uv** (one project at the repo root + `uv.lock`); use `uv add` / `uv run`, never pip. Docker images install with uv too.
Docker (`python:3.12-slim`), docker-compose for local development, minikube for Kubernetes (namespace `erm`).

## Layout (planned)
```
services/common/          shared models, partitioner, logging, correlation-id, AMQP + HTTP helpers
services/storage_node/    SQLite KV with versions, digest, sync
services/coordinator/     routing, replication, heartbeats, failover, resync, /dashboard
services/request_service/ requests API, state machine, publisher, reconciler
services/resource_service/ resources API, reserve/release (If-Match), cancel-event consumer
services/matching_service/ consumer, strategies, saga, POST /match/{id}
k8s/                      manifests
scripts/demo/             one script per spec §10 demo item
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
Fill these in during implementation (build, compose up, minikube deploy, tests, demo).

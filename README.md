# Distributed Emergency Resource Matching System

Cloud Computing project (Semester 7). People affected by a disaster create emergency requests (water, food, medicine, transportation, shelter); volunteers, NGOs and supply holders register resources. The system matches each request **asynchronously** (RabbitMQ) to the **nearest resource with enough quantity left**, never over-allocating, on top of a **sharded, replicated, self-healing** storage tier, deployed with **Docker and Kubernetes**.

```
client → Ingress → request-service ─┐            ┌→ matching-service ×N ─→ resource-service / request-service
                   resource-service ├→ RabbitMQ ─┘        (saga: reserve → assign → compensate)
                                    └→ coordinator → shard 0 [primary, replica]
                                                     shard 1 [primary, replica]
                                                     shard 2 [primary, replica]
```

| Document | For |
|---|---|
| [`DESIGN.md`](DESIGN.md) | requirements, architecture, ADRs, design patterns |
| [`HANDOVER.md`](HANDOVER.md) | status, how to run, demo runbook, what's left, viva notes |
| [`CLAUDE.md`](CLAUDE.md) | coding rules and commands |

## Quick start

```bash
make up        # docker compose: landing page + API on http://localhost:8080, dashboard on http://localhost:8090/dashboard
make test      # unit + integration tests

curl -X POST localhost:8080/resources -H 'Content-Type: application/json' \
  -d '{"type":"water","name":"Tanker","quantity":20,"location":{"lat":19.06,"lon":72.83}}'
curl -X POST localhost:8080/requests -H 'Content-Type: application/json' \
  -d '{"type":"water","quantity":4,"urgency":4,"location":{"lat":19.076,"lon":72.878}}'
curl localhost:8080/requests/<id>      # -> ASSIGNED, with distance_km
```

Kubernetes (minikube): `make k8s-start k8s-build k8s-deploy`, then `make k8s-gateway` and `make k8s-dashboard`.

## API

| Method | Endpoint | Purpose | Success |
|---|---|---|---|
| POST | `/requests` | create request | 201 |
| GET | `/requests/{id}` | retrieve request | 200 |
| GET | `/requests?status=&type=` | list requests | 200 |
| PUT | `/requests/{id}/status` | update status (`IN_PROGRESS`, `FULFILLED`, `CANCELLED`) | 200 |
| POST | `/resources` | register resource | 201 |
| GET | `/resources?type=&status=` | list resources | 200 |
| GET | `/resources/{id}` | retrieve resource | 200 |
| PUT | `/resources/{id}/status` | online/offline, restock | 200 |
| POST | `/match/{requestId}` | match on demand | 200 |

Errors: `{"error": {"code", "message", "correlation_id"}}` with 400 / 404 / 409 / 503. Interactive docs are at `/docs` on each service.

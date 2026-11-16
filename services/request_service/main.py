"""request-service HTTP API (spec Table 2: /requests...).

Public (through the Ingress):
  POST /requests                  createRequest      201
  GET  /requests/{id}             getRequest         200 (ETag = version)
  GET  /requests?status=&type=    list (demo helper) 200
  PUT  /requests/{id}/status      updateStatus       200 (optional If-Match)
Internal (matching-service only):
  POST /internal/requests/{id}/assign      saga step "assign" (If-Match)
  POST /internal/requests/{id}/unmatched   no resource available
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Response
from pydantic import BaseModel, Field

from services.common.amqp import AmqpClient
from services.common.http import parse_if_match, setup_app
from services.common.logging import setup_logging
from services.common.storage_client import StorageClient
from services.request_service.domain import Assignment, CreateRequest, EmergencyRequest, RequestStatus
from services.request_service.service import RequestService

log = setup_logging("request-service")
storage = StorageClient()
broker = AmqpClient()
service = RequestService(storage, broker)


@asynccontextmanager
async def lifespan(_: FastAPI):
    broker.start()
    yield
    await broker.close()
    await storage.close()


app = FastAPI(title="Request Service", lifespan=lifespan,
              description="Emergency requests: create, read, status lifecycle.")
setup_app(app)


def view(req: EmergencyRequest, version: int, response: Response | None = None) -> dict:
    if response is not None:
        response.headers["ETag"] = f'"{version}"'
    return {**req.model_dump(mode="json"), "version": version}


class StatusUpdate(BaseModel):
    status: RequestStatus
    note: str | None = Field(default=None, max_length=300)


@app.post("/requests", status_code=201)
async def create_request(body: CreateRequest, response: Response):
    req, version, queued = await service.create(body)
    response.headers["Location"] = f"/requests/{req.id}"
    return {**view(req, version, response), "queued_for_matching": queued}


@app.get("/requests/{request_id}")
async def get_request(request_id: str, response: Response):
    req, version = await service.get(request_id)
    return view(req, version, response)


@app.get("/requests")
async def list_requests(status: RequestStatus | None = None, type: str | None = None):
    reqs, partial = await service.list(status, type)
    return {"count": len(reqs), "partial": partial, "items": [r.model_dump(mode="json") for r in reqs]}


@app.put("/requests/{request_id}/status")
async def update_status(request_id: str, body: StatusUpdate, response: Response,
                        if_match: str | None = Header(default=None)):
    req, version = await service.update_status(request_id, body.status, body.note, parse_if_match(if_match))
    return view(req, version, response)


@app.post("/internal/requests/{request_id}/assign")
async def assign(request_id: str, body: Assignment, response: Response,
                 if_match: str | None = Header(default=None)):
    req, version = await service.assign(request_id, body, parse_if_match(if_match))
    return view(req, version, response)


@app.post("/internal/requests/{request_id}/unmatched")
async def unmatched(request_id: str, response: Response):
    req, version = await service.mark_unmatched(request_id)
    return view(req, version, response)


@app.get("/ready", include_in_schema=False)
async def ready():
    # Ready even without RabbitMQ: requests are still stored (graceful degradation, NFR-11).
    return {"status": "ok", "broker_connected": broker.is_ready}

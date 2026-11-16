"""resource-service HTTP API (spec Table 2: /resources...).

Public (through the Ingress):
  POST /resources                     registerResource   201
  GET  /resources?type=&status=       list               200 (scatter-gather across shards)
  GET  /resources/{id}                get                200
  PUT  /resources/{id}/status         update status      200 (optional If-Match)
Internal (matching-service only):
  POST /internal/resources/{id}/reserve   saga step "reserve"
  POST /internal/resources/{id}/release   saga compensation
Consumes: request.cancelled -> release
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Response
from pydantic import BaseModel, Field

from services.common.amqp import RELEASE_QUEUE, AmqpClient
from services.common.http import parse_if_match, setup_app
from services.common.logging import setup_logging
from services.common.storage_client import StorageClient
from services.resource_service.domain import RegisterResource, Resource, ResourceStatus
from services.resource_service.service import ResourceService

log = setup_logging("resource-service")
storage = StorageClient()
broker = AmqpClient()
service = ResourceService(storage, broker)
broker.add_consumer(RELEASE_QUEUE, service.on_request_cancelled)


@asynccontextmanager
async def lifespan(_: FastAPI):
    broker.start()
    yield
    await broker.close()
    await storage.close()


app = FastAPI(title="Resource Service", lifespan=lifespan,
              description="Volunteers, NGOs and supplies: registration, availability, reservations.")
setup_app(app)


def view(res: Resource, version: int, response: Response | None = None) -> dict:
    if response is not None:
        response.headers["ETag"] = f'"{version}"'
    return {**res.model_dump(mode="json"), "version": version}


class StatusUpdate(BaseModel):
    status: ResourceStatus | None = None
    quantity_remaining: int | None = Field(default=None, ge=0)


class ReserveBody(BaseModel):
    request_id: str
    quantity: int = Field(ge=1)


class ReleaseBody(BaseModel):
    request_id: str


@app.post("/resources", status_code=201)
async def register_resource(body: RegisterResource, response: Response):
    res, version = await service.register(body)
    response.headers["Location"] = f"/resources/{res.id}"
    return view(res, version, response)


@app.get("/resources")
async def list_resources(type: str | None = None, status: ResourceStatus | None = None):
    items, partial = await service.list(type, status)
    return {"count": len(items), "partial": partial, "items": [view(r, v) for r, v in items]}


@app.get("/resources/{resource_id}")
async def get_resource(resource_id: str, response: Response):
    res, version = await service.get(resource_id)
    return view(res, version, response)


@app.put("/resources/{resource_id}/status")
async def update_status(resource_id: str, body: StatusUpdate, response: Response,
                        if_match: str | None = Header(default=None)):
    res, version = await service.update_status(resource_id, body.status, body.quantity_remaining,
                                               parse_if_match(if_match))
    return view(res, version, response)


@app.post("/internal/resources/{resource_id}/reserve")
async def reserve(resource_id: str, body: ReserveBody, response: Response,
                  if_match: str | None = Header(default=None)):
    res, version = await service.reserve(resource_id, body.request_id, body.quantity, parse_if_match(if_match))
    return view(res, version, response)


@app.post("/internal/resources/{resource_id}/release")
async def release(resource_id: str, body: ReleaseBody, response: Response):
    res, version = await service.release(resource_id, body.request_id)
    return view(res, version, response)


@app.get("/ready", include_in_schema=False)
async def ready():
    return {"status": "ok", "broker_connected": broker.is_ready}

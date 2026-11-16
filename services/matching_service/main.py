"""matching-service: consumes `match.request` from RabbitMQ and serves POST /match/{requestId}.

Stateless, so it scales horizontally: every pod consumes from the same queue and RabbitMQ
hands each message to exactly one of them (Competing Consumers, DDIA ch. 12 "load balancing").
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.common.amqp import MATCHING_QUEUE, AmqpClient
from services.common.config import env_float, env_int, env_str
from services.common.errors import Conflict, NotFound
from services.common.http import setup_app
from services.common.logging import setup_logging
from services.matching_service.clients import RequestServiceClient, ResourceServiceClient
from services.matching_service.saga import MatchingSaga
from services.matching_service.strategies import get_strategy

log = setup_logging("matching-service")

strategy = get_strategy(env_str("MATCH_STRATEGY", "nearest"))
saga = MatchingSaga(RequestServiceClient(), ResourceServiceClient(), strategy,
                    max_radius_km=env_float("MAX_RADIUS_KM", 50.0))
broker = AmqpClient(prefetch=env_int("MATCHING_PREFETCH", 10))


async def on_match_command(message: dict) -> None:
    request_id = message.get("request_id")
    try:
        result = await saga.match(request_id)
        log.info(f"match command done: {result.outcome}", extra={"fields": result.to_json()})
    except NotFound:
        log.warning(f"request {request_id} does not exist; dropping message")


broker.add_consumer(MATCHING_QUEUE, on_match_command)


@asynccontextmanager
async def lifespan(_: FastAPI):
    broker.start()
    yield
    await broker.close()
    await saga.requests.close()
    await saga.resources.close()


app = FastAPI(title="Matching Service", lifespan=lifespan,
              description=f"Matches requests to resources. Strategy: {strategy.name}")
setup_app(app)


@app.post("/match/{request_id}")
async def match(request_id: str):
    """matchRequest(requestId): synchronous, on demand. 200 matched; 404 unknown request;
    409 NOT_MATCHABLE (already assigned/closed) or NO_RESOURCE_AVAILABLE."""
    result = await saga.match(request_id)
    if result.outcome == "not_matchable":
        raise Conflict(f"Request is {result.request_status}; it cannot be matched",
                       code="NOT_MATCHABLE", details=result.to_json())
    if result.outcome == "no_resource":
        raise Conflict(f"No suitable available resource within {saga.max_radius_km} km",
                       code="NO_RESOURCE_AVAILABLE", details=result.to_json())
    return {**result.to_json(), "strategy": strategy.name}


@app.get("/ready", include_in_schema=False)
async def ready():
    return {"status": "ok", "broker_connected": broker.is_ready, "strategy": strategy.name}

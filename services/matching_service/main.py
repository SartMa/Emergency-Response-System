"""matching-service: consumes `match.request` and supply events from RabbitMQ, and serves
POST /match/{requestId}.

Stateless, so it scales horizontally: every pod consumes from the same queue and RabbitMQ
hands each message to exactly one of them (Competing Consumers pattern).

Retries and wake-ups (FR-19, rules in policies.py):
  * no resource -> the command is re-published to erm.retry with a growing delay
  * a resource is registered / restocked / freed -> nearby UNMATCHED requests of that type
    get one more attempt
"""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from services.common.amqp import (COMMANDS_EXCHANGE, MATCH_ROUTING_KEY, MATCHING_QUEUE, RETRY_DELAYS_S,
                                  RETRY_EXCHANGE, WAKEUP_QUEUE, AmqpClient, PermanentError, retry_routing_key)
from services.common.config import env_float, env_int, env_str
from services.common.errors import Conflict, NotFound
from services.common.http import setup_app
from services.common.logging import setup_logging
from services.matching_service.clients import RequestServiceClient, ResourceServiceClient
from services.matching_service.policies import Supply, WaitingRequest, next_retry_delay, select_for_wakeup
from services.matching_service.saga import MatchingSaga
from services.matching_service.strategies import get_strategy

log = setup_logging("matching-service")

strategy = get_strategy(env_str("MATCH_STRATEGY", "nearest"))
saga = MatchingSaga(RequestServiceClient(), ResourceServiceClient(), strategy,
                    max_radius_km=env_float("MAX_RADIUS_KM", 50.0))
broker = AmqpClient(prefetch=env_int("MATCHING_PREFETCH", 10))
WAKEUP_BATCH = env_int("WAKEUP_BATCH", 50)


async def on_match_command(message: dict) -> None:
    request_id = message.get("request_id")
    attempt = int(message.get("attempt", 0))
    try:
        result = await saga.match(request_id)
        log.info(f"match command done: {result.outcome}", extra={"fields": {**result.to_json(), "attempt": attempt}})
    except NotFound:
        log.warning(f"request {request_id} does not exist; dropping message")
        return

    if result.outcome == "no_resource":
        delay = next_retry_delay(attempt, RETRY_DELAYS_S)
        if delay is None:
            log.info(f"request {request_id}: retries used up; it stays UNMATCHED until new supply wakes it")
            return
        # If this publish fails, the handler raises Unavailable and the message is requeued:
        # the saga runs again (idempotent) and we try to schedule the retry again.
        # Priority 0: retries go to the back of the line. Fresh requests (priority = urgency 1..5)
        # are always matched first, so a backlog of retries can't keep new requests in PENDING.
        await broker.publish(RETRY_EXCHANGE, retry_routing_key(delay), {**message, "attempt": attempt + 1},
                             priority=0)
        log.info(f"request {request_id}: no resource, retry {attempt + 1}/{len(RETRY_DELAYS_S)} in {delay}s")


WAKEUP_WINDOW_S = env_float("WAKEUP_WINDOW_S", 1.0)
_pending_supply: dict[str, list[Supply]] = {}  # type -> supply events collected in the current window
_wakeup_tasks: set[asyncio.Task] = set()       # keep references so the tasks aren't garbage-collected


async def on_supply_event(event: dict) -> None:
    """A resource appeared, was restocked, or got quantity back. Collect the event; one scan per
    type per WAKEUP_WINDOW_S (coalescing) then decides which waiting requests to wake up.
    Coalescing matters during bursts (e.g. many cancellations): one scan instead of one per event.
    Trade-off: the event is acknowledged before the scan runs, so if this pod dies within the
    window the wake-up is lost. The requests then simply keep waiting for the next supply event."""
    type_ = event.get("type")
    if not type_:
        raise PermanentError(f"supply event without a type: {event}")
    units = int(event.get("quantity_remaining", WAKEUP_BATCH))
    first_in_window = type_ not in _pending_supply
    _pending_supply.setdefault(type_, []).append(Supply(event.get("lat"), event.get("lon"), units))
    if first_in_window:
        task = asyncio.create_task(_wake_up_after_window(type_))
        _wakeup_tasks.add(task)
        task.add_done_callback(_wakeup_tasks.discard)


async def _wake_up_after_window(type_: str) -> None:
    await asyncio.sleep(WAKEUP_WINDOW_S)
    supplies = _pending_supply.pop(type_, [])
    try:
        waiting = [
            WaitingRequest(r["id"], r["type"], r["location"]["lat"], r["location"]["lon"], r["urgency"],
                           r["created_at"], r["quantity"])
            for r in await saga.requests.list_unmatched(type_)
        ]
        chosen = select_for_wakeup(waiting, type_, supplies, saga.max_radius_km, WAKEUP_BATCH)
        for r in chosen:
            # attempt = budget used up: a wake-up is one extra chance, not a fresh 5-retry chain.
            await broker.publish(COMMANDS_EXCHANGE, MATCH_ROUTING_KEY,
                                 {"request_id": r.id, "type": r.type, "urgency": r.urgency,
                                  "attempt": len(RETRY_DELAYS_S), "reason": "wake-up"},
                                 priority=r.urgency)
        if chosen:
            log.info(f"wake-up: {len(chosen)} waiting {type_} request(s) re-queued "
                     f"after {len(supplies)} supply event(s)")
    except Exception as exc:  # storage/broker briefly unavailable: requests keep waiting for the next event
        log.warning(f"wake-up scan for {type_} failed: {exc!r}")


broker.add_consumer(MATCHING_QUEUE, on_match_command)
broker.add_consumer(WAKEUP_QUEUE, on_supply_event)


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

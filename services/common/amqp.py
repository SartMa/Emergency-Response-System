"""RabbitMQ adapter: topology, publishing and consuming (DESIGN.md §6.1, ADR-10).

Topology:

  erm.commands (direct) --match.request--> matching.q   [durable, x-max-priority=5, DLX=erm.dlx]
  erm.retry    (direct) --retry.<N>s--> matching.retry.<N>s   [x-message-ttl=N s, dead-letters back
                                                                to erm.commands/match.request]
  erm.events   (topic)  --request.cancelled--> resource.release.q          [DLX=erm.dlx]
                        --resource.registered|restocked|released--> matching.wakeup.q  [DLX=erm.dlx]
  erm.dlx      (fanout) --> erm.dlq   (poison messages, inspected by hand)

Delayed retry (FR-19) uses ONE QUEUE PER DELAY, each with a queue-level TTL. A message waits in
matching.retry.8s for 8 s, expires, and RabbitMQ dead-letters it back onto matching.q. Why not
one retry queue with a per-message TTL? RabbitMQ only expires messages at the HEAD of a queue,
so a 2 s retry queued behind a 32 s one would wait 32 s (head-of-line blocking). With one queue
per delay, every message in a queue has the same TTL, so the head always expires first.

Every service declares the whole topology on startup. Declarations are idempotent, so the
start order of the services does not matter.

The connection is "robust": aio-pika reconnects by itself if RabbitMQ restarts, and restores
consumers. Startup never blocks on RabbitMQ: we connect in the background, and a request
created while RabbitMQ is down is still stored (graceful degradation, NFR-11).
"""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection

from services.common.config import RABBITMQ_URL
from services.common.errors import Unavailable
from services.common.logging import correlation_id, new_correlation_id

log = logging.getLogger("amqp")

COMMANDS_EXCHANGE = "erm.commands"
EVENTS_EXCHANGE = "erm.events"
DLX_EXCHANGE = "erm.dlx"
RETRY_EXCHANGE = "erm.retry"

MATCH_ROUTING_KEY = "match.request"
MATCHING_QUEUE = "matching.q"
RELEASE_QUEUE = "resource.release.q"
WAKEUP_QUEUE = "matching.wakeup.q"
DEAD_LETTER_QUEUE = "erm.dlq"

MAX_PRIORITY = 5  # urgency 1..5 maps directly onto message priority (FR-16)
PUBLISH_TIMEOUT_S = 2.0  # a healthy broker confirms in milliseconds

# Backoff schedule for "no resource right now" (FR-19): exponential, ~1 minute in total.
# Queue names contain the delay, so changing this list creates new queues instead of clashing
# with the arguments of existing ones (RabbitMQ refuses to redeclare a queue with new arguments).
RETRY_DELAYS_S = (2, 4, 8, 16, 32)

# Supply changes that can turn an UNMATCHED request into a matchable one (wake-up).
SUPPLY_EVENTS = ("resource.registered", "resource.restocked", "resource.released")


def retry_routing_key(delay_s: int) -> str:
    return f"retry.{delay_s}s"

Handler = Callable[[dict], Awaitable[None]]


class PermanentError(Exception):
    """Raise from a handler when retrying cannot help (bad message). The message goes to the DLQ."""


async def declare_topology(channel: aio_pika.abc.AbstractChannel) -> None:
    dlx = await channel.declare_exchange(DLX_EXCHANGE, aio_pika.ExchangeType.FANOUT, durable=True)
    dlq = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    await dlq.bind(dlx)

    commands = await channel.declare_exchange(COMMANDS_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    matching_q = await channel.declare_queue(
        MATCHING_QUEUE,
        durable=True,
        arguments={"x-max-priority": MAX_PRIORITY, "x-dead-letter-exchange": DLX_EXCHANGE},
    )
    await matching_q.bind(commands, routing_key=MATCH_ROUTING_KEY)

    events = await channel.declare_exchange(EVENTS_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    release_q = await channel.declare_queue(
        RELEASE_QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX_EXCHANGE}
    )
    await release_q.bind(events, routing_key="request.cancelled")

    # Delayed retries: one queue per delay; expired messages go back to matching.q.
    retry = await channel.declare_exchange(RETRY_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True)
    for delay in RETRY_DELAYS_S:
        q = await channel.declare_queue(
            f"matching.retry.{delay}s",
            durable=True,
            arguments={
                "x-message-ttl": delay * 1000,
                "x-dead-letter-exchange": COMMANDS_EXCHANGE,
                "x-dead-letter-routing-key": MATCH_ROUTING_KEY,
            },
        )
        await q.bind(retry, routing_key=retry_routing_key(delay))

    # Wake-up: matching hears about new or freed supply.
    wakeup_q = await channel.declare_queue(
        WAKEUP_QUEUE, durable=True, arguments={"x-dead-letter-exchange": DLX_EXCHANGE}
    )
    for key in SUPPLY_EVENTS:
        await wakeup_q.bind(events, routing_key=key)


class AmqpClient:
    def __init__(self, url: str = RABBITMQ_URL, prefetch: int = 10):
        self.url = url
        self.prefetch = prefetch
        self._connection: AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._consumers: list[tuple[str, Handler]] = []
        self._task: asyncio.Task | None = None
        self.connected = asyncio.Event()

    def add_consumer(self, queue: str, handler: Handler) -> None:
        """Register before start(); consumption begins once connected."""
        self._consumers.append((queue, handler))

    def start(self) -> None:
        self._task = asyncio.create_task(self._connect_loop())

    async def _connect_loop(self) -> None:
        delay = 1.0
        while True:
            try:
                self._connection = await aio_pika.connect_robust(self.url, timeout=5)
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=self.prefetch)
                await declare_topology(self._channel)
                for queue_name, handler in self._consumers:
                    queue = await self._channel.get_queue(queue_name)
                    await queue.consume(self._wrap(handler))
                self.connected.set()
                log.info("connected to RabbitMQ", extra={"fields": {"consumers": [q for q, _ in self._consumers]}})
                return
            except Exception as exc:  # RabbitMQ not up yet: keep trying in the background
                log.warning(f"RabbitMQ not reachable ({exc.__class__.__name__}), retrying in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10)

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
        if self._connection:
            await self._connection.close()

    @property
    def is_ready(self) -> bool:
        return self.connected.is_set() and self._connection is not None and not self._connection.is_closed

    async def publish(self, exchange: str, routing_key: str, body: dict, priority: int = 0) -> None:
        if not self.is_ready or self._channel is None:
            raise Unavailable("Message broker unavailable", code="BROKER_UNAVAILABLE")
        cid = correlation_id.get()
        message = aio_pika.Message(
            body=json.dumps(body).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  # survives a broker restart
            priority=priority,
            correlation_id=None if cid == "-" else cid,
        )
        try:
            # Time limit: while a robust connection is reconnecting, publish() waits for the
            # broker instead of failing, which would hang the caller's HTTP request until the
            # Ingress gives up (found in testing). Failing fast keeps NFR-11 true: the request is
            # stored, the client gets 201 with queued_for_matching=false, and the reconciler
            # re-sends it once RabbitMQ is back. If the cancelled publish did go out after all,
            # the resulting duplicate is harmless (idempotent consumer).
            async def _publish():
                ex = await self._channel.get_exchange(exchange)
                await ex.publish(message, routing_key=routing_key)

            await asyncio.wait_for(_publish(), timeout=PUBLISH_TIMEOUT_S)
        except Exception as exc:
            raise Unavailable(f"Publish failed: {exc.__class__.__name__}", code="BROKER_UNAVAILABLE")

    def _wrap(self, handler: Handler) -> Callable[[AbstractIncomingMessage], Awaitable[None]]:
        """Manual-ack consumer (CLAUDE.md rule):
        - success          -> ack
        - Unavailable      -> a dependency is briefly down (e.g. a shard failing over):
                              wait 1 s, then requeue so it is tried again
        - PermanentError   -> nack(requeue=False) -> DLQ
        - other exception  -> nack(requeue=False) -> DLQ (Tier 2 adds a retry queue with backoff)
        Handlers must be idempotent: RabbitMQ delivers at-least-once."""

        async def on_message(message: AbstractIncomingMessage) -> None:
            token = correlation_id.set(message.correlation_id or new_correlation_id())
            try:
                payload = json.loads(message.body)
                await handler(payload)
                await message.ack()
            except Unavailable as exc:
                log.warning(f"dependency unavailable, requeueing message: {exc.message}")
                await asyncio.sleep(1.0)
                await message.nack(requeue=True)
            except Exception as exc:
                level = logging.WARNING if isinstance(exc, PermanentError) else logging.ERROR
                log.log(level, f"message handling failed, sending to DLQ: {exc!r}",
                        exc_info=not isinstance(exc, PermanentError))
                await message.nack(requeue=False)
            finally:
                correlation_id.reset(token)

        return on_message

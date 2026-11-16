"""Structured JSON logging with a correlation ID (ADR-13, Correlation Identifier pattern).

Every log line is one JSON object, so `kubectl logs ... | grep <correlation_id>` follows a
single emergency request through request-service -> RabbitMQ -> matching-service -> storage.
"""

import contextvars
import json
import os
import logging
import sys
import time
import uuid

# The correlation ID for the work currently being done (an HTTP request or an AMQP message).
# A ContextVar is per-asyncio-task, so concurrent requests never mix their IDs.
correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="-")

CORRELATION_HEADER = "X-Correlation-ID"


def new_correlation_id() -> str:
    return uuid.uuid4().hex[:16]


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, node: str):
        super().__init__()
        self.service = service
        self.node = node

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": self.service,
            "node": self.node,
            "correlation_id": correlation_id.get(),
            "msg": record.getMessage(),
        }
        # Extra structured fields: log.info("stored", extra={"fields": {"key": k}})
        fields = getattr(record, "fields", None)
        if fields:
            entry.update(fields)
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(service: str, node: str | None = None, level: str = "INFO") -> logging.Logger:
    # Default node name = container hostname (the pod name in Kubernetes), e.g. matching-service-7c9f-x2k.
    node = node or os.environ.get("HOSTNAME", "-")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service, node))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # uvicorn's access log is noisy (heartbeats every 2 s); our middleware logs what matters.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("aiormq").setLevel(logging.WARNING)
    return logging.getLogger(service)

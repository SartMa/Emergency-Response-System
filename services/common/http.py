"""HTTP plumbing shared by all services: correlation-ID middleware, health endpoint, and an
httpx client that forwards the correlation ID on every outgoing call (CLAUDE.md rule)."""

import logging
import time

import httpx
from fastapi import FastAPI, Request

from services.common.config import HTTP_TIMEOUT_S
from services.common.errors import install_error_handlers
from services.common.logging import CORRELATION_HEADER, correlation_id, new_correlation_id

log = logging.getLogger("http")

# Paths polled constantly; logging them would drown everything else.
QUIET_PATHS = ("/health", "/ready", "/internal/nodes/heartbeat", "/internal/cluster", "/dashboard")


def setup_app(app: FastAPI) -> None:
    install_error_handlers(app)

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        cid = request.headers.get(CORRELATION_HEADER) or new_correlation_id()
        token = correlation_id.set(cid)
        started = time.perf_counter()
        try:
            response = await call_next(request)
            response.headers[CORRELATION_HEADER] = cid
            if not request.url.path.startswith(QUIET_PATHS):
                log.info(
                    f"{request.method} {request.url.path} -> {response.status_code}",
                    extra={"fields": {"ms": round((time.perf_counter() - started) * 1000, 1)}},
                )
            return response
        finally:
            correlation_id.reset(token)

    @app.get("/health", include_in_schema=False)
    async def health():
        # Liveness probe: the process is up and serving HTTP.
        return {"status": "ok"}


async def _add_correlation_header(request: httpx.Request) -> None:
    cid = correlation_id.get()
    if cid != "-":
        request.headers[CORRELATION_HEADER] = cid


def make_client(base_url: str = "", timeout: float = HTTP_TIMEOUT_S) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(timeout, connect=min(1.0, timeout)),
        event_hooks={"request": [_add_correlation_header]},
    )


def parse_if_match(if_match: str | None) -> int | None:
    """ETag/If-Match carry the storage version: `"7"` (quotes and W/ prefix tolerated).
    Exposing the version as an ETag makes optimistic concurrency part of the HTTP API (ADR-06)."""
    from services.common.errors import BadRequest

    if if_match is None:
        return None
    try:
        return int(if_match.replace("W/", "").strip().strip('"'))
    except ValueError:
        raise BadRequest('If-Match must be a version number, e.g. "3"')

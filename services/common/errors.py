"""One error format for every service (CLAUDE.md):

    {"error": {"code": "...", "message": "...", "correlation_id": "..."}}

400 invalid input, 404 missing, 409 conflict / invalid transition, 503 dependency unavailable.
"""

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from services.common.logging import correlation_id


class AppError(Exception):
    status = 500
    code = "INTERNAL"

    def __init__(self, message: str, code: str | None = None, details: dict | None = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code
        self.details = details or {}


class BadRequest(AppError):
    status = 400
    code = "INVALID_REQUEST"


class NotFound(AppError):
    status = 404
    code = "NOT_FOUND"


class Conflict(AppError):
    status = 409
    code = "CONFLICT"


class VersionConflict(Conflict):
    """Optimistic concurrency failure: somebody else wrote first (ADR-06)."""

    code = "VERSION_CONFLICT"


class Unavailable(AppError):
    status = 503
    code = "UNAVAILABLE"


def error_body(code: str, message: str, details: dict | None = None) -> dict:
    body = {"code": code, "message": message, "correlation_id": correlation_id.get()}
    if details:
        body["details"] = details
    return {"error": body}


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def _app_error(_: Request, exc: AppError):
        return JSONResponse(error_body(exc.code, exc.message, exc.details), status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError):
        # FastAPI defaults to 422; the spec talks about "invalid requests", so we use 400.
        problems = [
            {"field": ".".join(str(p) for p in e["loc"][1:]), "problem": e["msg"]}
            for e in exc.errors()
        ]
        return JSONResponse(
            error_body("INVALID_REQUEST", "Request body or parameters are invalid", {"problems": problems}),
            status_code=400,
        )


def raise_for_error_response(response) -> None:
    """Turn an error response from another of our services back into the matching AppError,
    so errors keep their meaning as they travel between services."""
    if response.status_code < 400:
        return
    try:
        err = response.json()["error"]
        code, message = err.get("code"), err.get("message", "")
        details = err.get("details")
    except Exception:
        code, message, details = None, response.text[:200], None
    by_status = {400: BadRequest, 404: NotFound, 409: Conflict, 412: VersionConflict, 503: Unavailable}
    cls = by_status.get(response.status_code, AppError)
    if response.status_code == 409 and code == "VERSION_CONFLICT":
        cls = VersionConflict
    raise cls(message or f"HTTP {response.status_code}", code=code, details=details)

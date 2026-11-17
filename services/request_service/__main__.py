"""Entry point: python -m services.request_service"""

import uvicorn

from services.common.config import env_int

if __name__ == "__main__":
    uvicorn.run(
        "services.request_service.main:app",
        host="0.0.0.0",
        port=env_int("PORT", 8000),
        log_config=None,  # keep our JSON log format instead of uvicorn's default one
        # Keep idle connections open longer than any client does (nginx Ingress: 60 s, httpx: 5 s).
        # With uvicorn's default of 5 s, a client could send a request on a connection the server
        # was just closing: the Ingress answered 502, and inside the coordinator the reset looked
        # like a dead primary (a false failover). Found in a chaos run.
        timeout_keep_alive=75,
    )

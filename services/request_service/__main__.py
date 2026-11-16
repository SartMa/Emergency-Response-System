"""Entry point: python -m services.request_service"""

import uvicorn

from services.common.config import env_int

if __name__ == "__main__":
    # log_config=None: keep our JSON log format instead of uvicorn's default one.
    uvicorn.run("services.request_service.main:app", host="0.0.0.0", port=env_int("PORT", 8000), log_config=None)

"""Configuration helpers.

All configuration comes from environment variables (CONTRIBUTING.md rule). In Kubernetes they are
injected from a ConfigMap; in docker-compose they are set in docker-compose.yml.
"""

import os


def env_str(name: str, default: str | None = None) -> str:
    raw = os.environ.get(name, default)
    if raw is None:
        raise RuntimeError(f"Missing required environment variable {name}")
    return raw


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# Shared settings used by more than one component.
SERVICE_NAME = env_str("SERVICE_NAME", "unknown")
COORDINATOR_URL = env_str("COORDINATOR_URL", "http://coordinator:8000")
RABBITMQ_URL = env_str("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
REQUEST_SERVICE_URL = env_str("REQUEST_SERVICE_URL", "http://request-service:8000")
RESOURCE_SERVICE_URL = env_str("RESOURCE_SERVICE_URL", "http://resource-service:8000")
HTTP_TIMEOUT_S = env_float("HTTP_TIMEOUT_S", 3.0)

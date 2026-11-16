# One Dockerfile, one image per service:  docker build --build-arg SERVICE=coordinator -t erm/coordinator .
# All services share the `services/common` package, so they share the dependency layer too.
# That makes builds fast and minikube image loading cheap, while each service still ships and
# scales as its own image/container.
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.4 /uv /usr/local/bin/uv

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1

# Dependencies first (cached unless pyproject/uv.lock change), then our code.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY services ./services

ARG SERVICE
ENV SERVICE=${SERVICE} \
    SERVICE_NAME=${SERVICE} \
    PATH="/app/.venv/bin:${PATH}" \
    PORT=8000
EXPOSE 8000

# Run as an unprivileged user; storage nodes write to /data (a volume).
RUN useradd --uid 1000 --create-home app && mkdir -p /data && chown app /data
USER app

CMD ["sh", "-c", "exec python -m services.${SERVICE}"]

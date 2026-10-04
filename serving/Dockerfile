FROM python:3.11-slim

# Pinned uv binary (matches the host's uv 0.10.8)
COPY --from=ghcr.io/astral-sh/uv:0.10.8 /uv /uvx /bin/

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies only, from the "serving" group (no pyspark/pandas/kafka) - cached until the lockfile changes
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --only-group serving

# Only the code the API imports
COPY storage/models.py storage/models.py
COPY observability/logging_config.py observability/logging_config.py
COPY serving/api serving/api

EXPOSE 8000

# Exec form so SIGTERM reaches uvicorn and lifespan shutdown runs
CMD ["uvicorn", "serving.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# pip only: this service needs two libraries, so no uv/lockfile machinery (D50).
# Versions match the host's resolved ones.
RUN pip install --no-cache-dir kafka-python==3.0.11 psycopg2-binary==2.9.13

WORKDIR /app

# The code is bind-mounted (./observability -> /app/observability), so edits need only a container restart.
CMD ["python", "-m", "observability.healthcheck"]
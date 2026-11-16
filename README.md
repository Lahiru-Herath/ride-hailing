# Ride-Hailing Fleet Operations: Lambda Data Pipeline

EC8203 Applied Big Data Engineering, mini-project (Use Case 1: Ride-Hailing Fleet Operations)

**Group:** Herath H M L B (EG/2021/4549) · Levmanthi K G M (EG/2021/4646) · Sandaruwan G G A (EG/2021/4782)

An end-to-end Lambda-architecture pipeline for a simulated ride-hailing fleet. The streaming path gives live fleet
utilisation, earnings by zone and idle-vehicle alerts. The batch path reconciles raw trips against daily cost files to
produce a per-vehicle profitability report. A serving layer (FastAPI + dashboard) merges both, and an observability
layer monitors every stage.

**Business question:** *What is fleet utilisation and what are earnings by area right now, and which vehicles are
becoming unprofitable once yesterday's fuel and maintenance costs are factored in?*

> **Simulated clock (assumption):** 1 simulated day = **5 real minutes (300 s)**. The producer emits one event per
> vehicle every **3 s** (15 vehicles). The batch generator writes one cost CSV per 300 s. All timestamps are UTC.

## Architecture

```
 streaming_producer.py ──► Kafka topic trip_events ──► Spark Structured Streaming ──┬─► raw Parquet (storage/raw_events)
   (15 vehicles, 3 s)        (1 partition, KRaft)         (spark container)         ├─► realtime_fleet_metrics (5-min zone windows)
                                                                                    └─► alerts (idle vehicle > 10 min)
 batch_generator.py ──► storage/batch_incoming/*.csv ──► Airflow DAG daily_reconciliation (every 5 min)
   (1 CSV / 300 s)                                          FileSensor → stream-ready check → reconcile → quality gate → archive
                                                                         └─► vehicle_profitability (idempotent upsert)

 FastAPI (api container) ◄── Postgres ◄── healthcheck service (every 15 s: Kafka offset, lag, Airflow runs → pipeline_health, alerts)
   /  /fleet/*  /alerts  /health  /metrics
```

| Layer | Component | Technology |
|---|---|---|
| Ingestion | Streaming producer, batch generator, broker | Python (kafka-python), Apache Kafka 4.1 (KRaft, no Zookeeper) |
| Speed layer | Raw sink, 5-minute zone windows, stateful idle detection | Spark 4.1 Structured Streaming |
| Batch layer | Daily reconciliation | Apache Airflow 3.3 (FileSensor, mapped tasks) + pandas/pyarrow |
| Storage | Raw store, serving tables | Parquet (partitioned by `event_date`), PostgreSQL 18, SQLAlchemy 2 + Alembic |
| Serving | REST API and live dashboard | FastAPI, single-file HTML dashboard |
| Observability | JSON logs, health checker, `/health`, `/metrics`, request IDs | Python, Postgres, FastAPI |

**Why Lambda, not Kappa:** the telemetry stream answers "what is happening now" (approximate, low latency), while the
daily cost file answers "what did yesterday actually cost" (exact, periodic). The batch layer recomputes trips from the
raw event store and so corrects any speed-layer duplicates or drops. The full argument is in the report; every design
decision is logged in [`docs/DECISIONS_LOG.md`](docs/DECISIONS_LOG.md).

### What each layer computes

- **Zone enrichment:** the city is a bounding box split into a 3×3 grid, zones `Z1`–`Z9`.
- **Completed trip:** an event with `status = 'idle'` and a non-null `trip_id`; it carries the finalised fare.
- **Speed layer:** 5-minute tumbling windows with a 2-minute watermark, per zone: active vehicles, idle vehicles,
  completed trips, earnings. A stateful per-vehicle processor raises one alert per idle episode after 10 minutes.
- **Batch layer:** per vehicle, `net_profit = earnings − fuel_cost − maintenance_cost`, `is_profitable = net_profit > 0`,
  `cost_per_km = (fuel + maintenance) / distance`. Results are upserted on `(report_date, vehicle_id)`, so re-runs are
  idempotent.

## Prerequisites

- Docker with Compose v2 (tested on Ubuntu with native Docker Engine; with Docker Desktop installed, make sure
  `docker context` points at a running engine)
- [uv](https://docs.astral.sh/uv/) and Python 3.11+
- About 6 GB of free RAM for the containers
- Windows / Git Bash: prefix `docker exec <container> /absolute/path ...` with `MSYS_NO_PATHCONV=1`

## Quick start

All commands run from the project root.

```bash
# 1. configuration and Python environment
cp .env.example .env
uv sync

# 2. password file for the Airflow API login used by the health checker (development only)
echo '{"admin": "admin"}' > orchestration/simple_auth_passwords.json && chmod 666 orchestration/simple_auth_passwords.json

# 3. build images and start Postgres + Kafka
docker compose build
docker compose up -d kafka postgres
sleep 20

# 4. one-time database setup (Airflow metadata DB, then the schema via Alembic)
docker exec postgres psql -U pipeline -d postgres -c "CREATE DATABASE airflow_meta;"
uv run alembic upgrade head

# 5. Kafka topic
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic trip_events --partitions 1 --replication-factor 1

# 6. the rest of the stack (always -d: an attached `up` stops services when the terminal closes)
docker compose up -d spark airflow api healthcheck
sleep 90                       # Spark needs ~3 min to be fully ready (log line: all_queries_started)
docker exec airflow airflow dags unpause daily_reconciliation

# 7. data sources (host processes; tmux keeps them alive after a disconnect)
tmux new -d -s producer "uv run python sources/streaming_producer.py"
tmux new -d -s batchgen "uv run python sources/batch_generator.py"
```

Open **http://localhost:8000** (dashboard), **http://localhost:8000/docs** (API docs) and **http://localhost:8080**
(Airflow, login `admin` / `admin`). Windows appear after about 8 minutes (5-minute window plus 2-minute watermark); the
first batch result appears 5–6 minutes after the generator starts. If `storage/batch_incoming/` holds CSVs from an
earlier run, delete them before starting.

## Verify it works

```bash
docker compose ps                                        # all services Up
curl -s localhost:8000/health | python3 -m json.tool     # pipeline.status becomes "ok" once data flows
curl -s localhost:8000/fleet/overview | python3 -m json.tool

# speed layer: newest windows
docker exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT window_start, zone, active_vehicles, idle_vehicles, trips_completed, total_earnings FROM realtime_fleet_metrics ORDER BY window_start DESC, zone LIMIT 18;"

# batch layer: profitability per simulated day
docker exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT report_date, count(*) vehicles, sum(trips_completed) trips, count(*) FILTER (WHERE is_profitable) profitable FROM vehicle_profitability GROUP BY report_date ORDER BY report_date DESC LIMIT 5;"

# alerts (idle vehicles and pipeline health)
docker exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT id, entity_id, alert_type, severity, message, triggered_at, resolved FROM alerts ORDER BY id DESC LIMIT 10;"
```

## API

| Endpoint | Purpose |
|---|---|
| `GET /` | Live dashboard (polls every 5 s) |
| `GET /fleet/realtime?windows=6` | Speed layer: newest 5-minute windows by zone |
| `GET /fleet/profitability?days=12` | Batch layer: latest simulated day per vehicle plus daily history |
| `GET /fleet/profitability/{vehicle_id}` | One vehicle's results over the last days |
| `GET /fleet/overview` | Lambda merge: latest batch day plus speed windows after the batch cut-off, with freshness |
| `GET /alerts` | Idle-vehicle and pipeline alerts (`active` flag) |
| `GET /health` | API and DB reachability plus pipeline health (Kafka, producer, speed layer, batch layer, Airflow DAG) |
| `GET /metrics` | Prometheus text format: request counters and latency, component status, layer lag, open alerts |

## Observability

- **Structured logging:** every stage (producer, Spark job, Airflow tasks, API, health checker) writes one JSON object
  per line through `observability/logging_config.py` (read with `docker logs <service>`).
- **Request IDs:** each API request has an `X-Request-ID` (client-supplied or generated). It is returned in the
  response header and attached to every log line written for that request; a 500 returns it in the body so it can be
  matched to the logged traceback. Across stages, records are correlated by `trip_id`, `vehicle_id` and event time.
- **Health checker** (`observability/healthcheck.py`, the `healthcheck` service) runs every 15 s and upserts one row per
  component into `pipeline_health`: `kafka`, `producer` (topic end offset stopped advancing: degraded at 30 s, down at
  60 s), `speed_layer` and `batch_layer` (data lag, judged only while the producer is healthy), `airflow_dag` (failed or
  stuck runs, via the Airflow REST API). A degraded or down component opens one alert; recovery resolves it.
- **Data-quality gate:** the DAG fails if the day's rows are missing, differ in count from the CSV, or have nulls in key
  columns, and the CSV is archived only after the check passes.
- **Metrics:** `/metrics`; `/health` and the dashboard header chip show the same state.

Try it: stop the producer (`tmux kill-session -t producer`) and watch an alert open at about 60 s, then restart it and
watch it resolve. To see a failed Airflow run, temporarily add `raise RuntimeError("test")` at the top of
`processing/batch_reconciliation.py`, wait for the next scheduled run, and revert it afterwards.

## Tests

```bash
uv add --dev pytest        # first time only
uv run pytest tests -v     # health-check decision logic, Airflow client, JSON logging (22 tests)
```

## Repository layout

```
.
├── docker-compose.yml, pyproject.toml, uv.lock, alembic.ini, .env.example
├── sources/          streaming_producer.py, batch_generator.py
├── processing/       speed_layer_job.py (Spark), batch_reconciliation.py, Dockerfile
├── orchestration/    dags/daily_reconciliation_dag.py, Dockerfile
├── storage/          models.py, migrations/ (Alembic); generated: raw_events/, batch_incoming/, batch_processed/, checkpoints/
├── serving/          api/main.py, api/dashboard.html, Dockerfile
├── observability/    logging_config.py, healthcheck.py, Dockerfile
├── tests/            pytest suite
└── docs/             DECISIONS_LOG.md
```

## Operating notes

- **Stop:** end the two sources (`tmux kill-session`), then `docker compose stop` (not `down -v`).
- **Full reset** (`docker compose down -v`): recreate `airflow_meta`, run `alembic upgrade head`, delete both
  `storage/checkpoints/*` and `storage/raw_events/` together (stale checkpoints that reference a wiped topic stop Spark
  from starting), then recreate the topic.

## Key design decisions

| # | Decision |
|---|---|
| D2 | Lambda over Kappa: different source semantics; batch recomputes from raw data |
| D7 | Kafka in KRaft mode (Zookeeper support removed in 4.0) |
| D17 | Append-mode windows with a watermark (plain INSERT, no upsert) |
| D20/D21 | Protobuf pinned to 6.33.0; RocksDB state store for `transformWithStateInPandas` |
| D25 | Earnings recomputed from raw Parquet, not from `realtime_fleet_metrics` |
| D27 | `vehicle_profitability` upsert on `(report_date, vehicle_id)` |
| D28 | FileSensor (reschedule, soft-fail) and archive-on-success |
| D30 | Data-quality gate before archiving |
| D46–D49 | Health checker as its own service; one `pipeline_health` row per component; producer-gated lag |
| D55–D56 | Request IDs for correlation; Prometheus-format `/metrics` without a Prometheus server |

The complete list (D1–D56) is in `docs/DECISIONS_LOG.md`.

## Status

All phases are complete: Docker stack and schema, ingestion, raw Parquet storage, Spark speed layer, batch
reconciliation with Airflow, FastAPI serving layer and dashboard, observability (health checker, `/health`, `/metrics`,
request IDs), and tests.

## Known limitations

- Single-node Kafka (one partition), Spark and Airflow; no fault tolerance. Airflow runs in `standalone` mode with the
  development-only Simple Auth Manager and a fixed login; the API is unauthenticated.
- No end-to-end exactly-once guarantee; the batch layer de-duplicates trips on `trip_id`.
- The batch layer runs on pandas, not Spark, and processes one CSV per run.
- Idle-vehicle alerts are never resolved by the speed layer (the API treats them as active for 15 minutes).
- The batch layer cannot tell "no trips" from "the stream was down" (the producer alert covers that); there is no
  log-error-rate alert.
- Correlation, not distributed tracing: no spans across Kafka and Spark.
- Costs in the synthetic batch data are independent of trip activity, so many vehicles appear unprofitable.
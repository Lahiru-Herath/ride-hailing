# Ride-Hailing Fleet Operations: Lambda Data Pipeline

EC8203 Applied Big Data Engineering: Mini Project

**Group:** Herath H M L B (EG/2021/4549) · Levmanthi K G M (EG/2021/4646) · Sandaruwan G G A (EG/2021/4782)

An end-to-end Lambda-architecture pipeline for a simulated ride-hailing fleet. A streaming path gives live fleet utilisation, earnings by zone and idle-vehicle alerts. A batch path reconciles raw trips against daily vehicle cost files to produce a per-vehicle profitability report.

> **Simulated clock:** 1 simulated day = **5 real minutes**. The producer emits an event for each of the 15 vehicles every **3 seconds**, and the batch generator writes one cost CSV every 300 seconds.

---

## Architecture

```
Streaming producer ──► Kafka (trip_events) ──► Spark Structured Streaming ──┬─► realtime_fleet_metrics (Postgres)
   (15 vehicles, 3 s)                          (single job, 3 queries)      ├─► alerts (Postgres)
                                                                            └─► raw Parquet  storage/raw_events/
                                                                                     │
Batch generator ──► storage/batch_incoming/*.csv ──► Airflow DAG ◄───────────────────┘
   (1 CSV / sim day)                                 (sensor → reconcile → quality check → archive)
                                                          │
                                                          ▼
                                                 vehicle_profitability (Postgres)
```

| Layer         | Component                                   | Technology                                                    |
| ------------- | ------------------------------------------- | ------------------------------------------------------------- |
| Ingestion     | Streaming producer, batch generator, broker | Python (kafka-python), Apache Kafka 4.1 (KRaft, no Zookeeper) |
| Speed layer   | Windowed metrics, idle detection, raw sink  | Spark 4.1 Structured Streaming                                |
| Batch layer   | Daily reconciliation                        | Airflow 3.3.2 + pandas/pyarrow                                |
| Storage       | Serving store, raw store                    | PostgreSQL 18, Parquet (partitioned by `event_date`)          |
| Schema        | Models and migrations                       | SQLAlchemy 2.0 + Alembic                                      |
| Serving       | REST API                                    | FastAPI (see [Status](#status))                               |
| Observability | Logs, health checks                         | Structured JSON logging (see [Status](#status))               |

**Why Lambda, not Kappa:** the telemetry stream answers "what is happening now" (approximate, low latency), while the daily cost file answers "what did yesterday actually cost" (exact, periodic). The batch layer independently recomputes trips from the raw event store, correcting any speed-layer duplicates or drops. The full argument is in the project report, and every design decision is logged in `02_DECISIONS_LOG.md` (D1–D30).

### What each layer computes

- **Zone enrichment:** the city is a bounding box split into a 3×3 grid, zones `Z1`–`Z9`.
- **Completed trip:** an event with `status = 'idle'` and a non-null `trip_id`. It carries the finalised fare. Earnings are summed only from these events.
- **Speed layer:** 5-minute tumbling windows with a 2-minute watermark, per zone: active vehicles, idle vehicles, completed trips, earnings. Metrics for a window appear about 5–7 minutes after it starts. A stateful per-vehicle processor raises one alert per idle episode after 10 minutes.
- **Batch layer:** per vehicle, `net_profit = earnings − fuel_cost − maintenance_cost`, `is_profitable = net_profit > 0`, `cost_per_km = (fuel + maintenance) / distance`. Vehicles with no trips get earnings 0. Results are upserted on `(report_date, vehicle_id)`, so re-runs are idempotent.

---

## Repository layout

```
.
├── docker-compose.yml            # Kafka, Postgres, Airflow, Spark
├── .env                          # host-side config (see below)
├── sources/
│   ├── streaming_producer.py     # simulated vehicle telemetry -> Kafka
│   └── batch_generator.py        # daily cost CSV -> storage/batch_incoming/
├── processing/
│   ├── Dockerfile                # Spark image (pandas, pyarrow, protobuf 6.33.0, psycopg2)
│   ├── speed_layer_job.py        # Spark Structured Streaming job (3 queries)
│   └── batch_reconciliation.py   # batch layer: raw Parquet + cost CSV -> vehicle_profitability
├── orchestration/
│   ├── Dockerfile                # Airflow image (+ pandas, pyarrow)
│   └── dags/daily_reconciliation_dag.py
├── storage/
│   ├── models.py                 # SQLAlchemy models
│   ├── migrations/               # Alembic
│   ├── raw_events/               # Parquet (generated)
│   ├── batch_incoming/           # daily CSVs (generated)
│   ├── batch_processed/          # archived CSVs (generated)
│   └── checkpoints/              # Spark checkpoints (generated)
└── observability/
    └── logging_config.py         # shared JSON logger
```

---

## Prerequisites

- Docker Desktop (Compose v2)
- [`uv`](https://docs.astral.sh/uv/) for Python dependencies
- Python 3.11+ (managed by `uv`)
- Roughly 6 GB of free RAM for the containers
- **Windows / Git Bash users:** prefix `docker compose exec <service> /absolute/path ...` with `MSYS_NO_PATHCONV=1`, otherwise Git Bash rewrites the path.

---

## Setup and run

All commands run from the project root.

### 1. Configure

Create `.env` (host-side scripts read it; containers get their settings from `docker-compose.yml`):

```ini
# Simulated clock
SIM_DAY_DURATION_SECONDS=300
STREAM_EVENT_INTERVAL_SECONDS=3

# Kafka (host scripts use the PLAINTEXT_HOST listener)
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_TOPIC_TRIP_EVENTS=trip_events

# Postgres (host port 5433 -> container 5432)
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=ridehailing
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline
DATABASE_URL=postgresql+psycopg2://pipeline:pipeline@localhost:5433/ridehailing

# Fleet
NUM_VEHICLES=15
IDLE_ALERT_THRESHOLD_MINUTES=10

# Batch layer
BATCH_OUTPUT_DIR=storage/batch_incoming
```

### 2. Install Python dependencies

```bash
uv sync
```

### 3. Start the infrastructure

```bash
docker compose up -d postgres kafka
```

Create the Airflow metadata database (needed once, after any volume wipe):

```bash
docker compose exec postgres psql -U pipeline -d postgres -c "CREATE DATABASE airflow_meta;"
```

### 4. Create the schema

```bash
uv run alembic upgrade head
```

### 5. Start Airflow and Spark

```bash
docker compose up -d --build airflow spark
```

The first Spark start is slow (it resolves the Kafka connector and copies jars); wait until the log shows `all_queries_started`:

```bash
docker compose logs --since 10m spark | grep -E "sink_started|all_queries_started|Exception|ERROR"
```

### 6. Start the data sources (two separate terminals)

```bash
uv run python sources/streaming_producer.py
```

```bash
uv run python sources/batch_generator.py
```

The producer emits events immediately. The first cost CSV appears in `storage/batch_incoming/` after about 5 minutes, named `vehicle_costs_<UTC window end>.csv`.

### 7. Open Airflow and run the DAG

Open <http://localhost:8080>, user `admin`. Airflow 3 uses the Simple Auth Manager and generates the password; fetch it with:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

The password changes whenever the container is recreated. In the UI, **unpause** `daily_reconciliation` (new DAGs start paused); it then runs every 5 minutes, or click **Trigger** for an immediate run. Each run processes the oldest pending CSV and moves it to `storage/batch_processed/` only after the data-quality check passes.

---

## Verifying results

**Streaming metrics (appear ~5–7 minutes after the producer starts):**

```bash
docker compose exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT window_start, zone, active_vehicles, idle_vehicles, trips_completed, total_earnings FROM realtime_fleet_metrics ORDER BY window_start DESC, zone LIMIT 18;"
```

**Idle-vehicle alerts (first ones after ~10 minutes):**

```bash
docker compose exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT entity_id, alert_type, severity, message, triggered_at FROM alerts ORDER BY triggered_at DESC LIMIT 10;"
```

**Daily profitability report:**

```bash
docker compose exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT vehicle_id, trips_completed, total_earnings, fuel_cost, maintenance_cost, net_profit, is_profitable, cost_per_km FROM vehicle_profitability ORDER BY report_date DESC, vehicle_id LIMIT 15;"
```

**Stream freshness (lag of newest window; ~7 min is normal = 5-min window + 2-min watermark):**

```bash
docker compose exec postgres psql -U pipeline -d ridehailing -c \
  "SELECT date_trunc('second', (now() AT TIME ZONE 'utc') - max(window_end)) AS lag FROM realtime_fleet_metrics;"
```

**Run the batch job by hand against one CSV (host, no Airflow):**

```bash
uv run python processing/batch_reconciliation.py storage/batch_incoming/vehicle_costs_<stamp>.csv
```

Add `--archive` to also move the file to `storage/batch_processed/` after the quality check.

---

## Observability

- **Structured logging:** every component logs one JSON object per line (`timestamp`, `level`, `component`, `message`, plus event fields) via `observability/logging_config.py`. Examples: `event_emitted`, `metrics_batch_written`, `alerts_batch_written`, `reconciliation_complete`, `quality_check_passed`.
- **Business alert:** a vehicle idle for more than `IDLE_ALERT_THRESHOLD_MINUTES` writes one row to `alerts` (one per idle episode).
- **Data-quality gate:** the DAG fails if the day's rows are missing, differ in count from the CSV, or have nulls in key columns.
- **Health-check rules (design):** no new metrics window for N minutes; failed Airflow run; high error-log rate, recorded in `pipeline_health` / `alerts` and surfaced by `/health`.

---

## Stopping and resetting

```bash
docker compose down          # stop; data volumes (Postgres, Kafka) are kept
docker compose down -v       # stop and wipe all volumes (Postgres, Kafka, Ivy cache)
```

After `down -v`, also delete `storage/checkpoints/`, `storage/raw_events/`, `storage/batch_incoming/` and `storage/batch_processed/`, then repeat steps 3–6 (including `CREATE DATABASE airflow_meta` and `alembic upgrade head`). Stale Spark checkpoints that reference a wiped Kafka topic prevent the streaming job from starting.

---

## Design decisions worth knowing

| #       | Decision                                                                                |
| ------- | --------------------------------------------------------------------------------------- |
| D2      | Lambda over Kappa: different source semantics; batch recomputes from raw data           |
| D7      | Kafka KRaft mode (Zookeeper support removed in 4.0)                                     |
| D8      | Postgres 18 volume must be mounted at `/var/lib/postgresql`                             |
| D10     | SQLAlchemy + Alembic for schema; raw psycopg2 for streaming writes                      |
| D17     | Append-mode windows with watermark (plain INSERT, no upsert)                            |
| D20/D21 | Protobuf pinned to 6.33.0; RocksDB state store required by `transformWithStateInPandas` |
| D24     | Timestamped batch filenames + atomic write; day window derived from the filename        |
| D25     | Earnings recomputed from raw Parquet, not from `realtime_fleet_metrics`                 |
| D26     | Batch job runs as pandas/pyarrow inside a custom Airflow image                          |
| D27     | `vehicle_profitability` upsert on `(report_date, vehicle_id)`                           |
| D28     | FileSensor (reschedule, soft-fail) + archive-on-success                                 |
| D29     | Raw Parquet read through Spark's `_spark_metadata` sink log, not by directory listing   |
| D30     | Data-quality gate before archiving                                                      |

---

## Status

| Phase | Scope                                             | State                                                                          |
| ----- | ------------------------------------------------- | ------------------------------------------------------------------------------ |
| 0     | Docker stack, schema, Alembic                     | Complete                                                                       |
| 1     | Streaming producer, batch generator, JSON logging | Complete                                                                       |
| 2     | Raw Parquet storage                               | Complete                                                                       |
| 3     | Spark speed layer (windowed metrics, idle alerts) | Complete                                                                       |
| 4     | Batch reconciliation + Airflow DAG                | Reconciliation job verified; DAG deployed, final end-to-end run being verified |
| 5     | FastAPI serving layer                             | _Update before submission_                                                     |
| 6     | Health-check script                               | _Update before submission_                                                     |
| 7     | Automated tests, clean-start check                | _Update before submission_                                                     |

## Known limitations

- Single-node Kafka/Spark/Airflow; no fault tolerance (the streaming job must be restarted if it dies; consider `restart: on-failure` on the `spark` service).
- No end-to-end exactly-once guarantee; the batch layer de-duplicates trips on `trip_id`.
- The DAG waits a fixed grace period for the stream to flush and processes one CSV per run, so a backlog of files is not drained quickly.
- Costs in the synthetic batch data are independent of trip activity, so many vehicles appear unprofitable.
- The Kafka topic may have one partition (auto-created); recreate with 3 partitions (and clear checkpoints) if the partitioning claim matters.

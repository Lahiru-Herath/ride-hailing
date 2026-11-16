# Decisions Log

Running record of every non-trivial decision made so far, with reasoning, so the "why" survives a chat handoff and doubles as report material for the tech-stack-justification section.

**Numbering:** next free number is **D57**. D15 is intentionally unused (skipped during Phase 3; kept so existing references stay stable).

---

### D1 — Use case: Ride-Hailing Fleet Operations

Simplest schema to simulate believably, clearest alerting story (idle vehicle threshold), least domain-expertise overhead vs. Hospital Vitals or Smart Grid alternatives that were also on the table.

### D2 — Architecture: Lambda (not Kappa)

The two sources (streaming trips, daily cost file) have genuinely different semantics — one answers "what's happening now," the other "what did yesterday actually cost, authoritatively." Lambda's speed+batch split matches this naturally; forcing the batch file through a Kappa replay pipeline would add complexity without benefit since it isn't naturally an event stream. This is worth ~20 rubric marks — the report must argue this explicitly, not just state it.

### D3 — Spark Structured Streaming over Storm

Better documentation, Python-native, same engine can conceptually serve both speed-layer streaming and batch reconciliation jobs. (Note: the batch layer ended up on pandas/pyarrow, see D26.)

### D4 — PostgreSQL over Cassandra

No write-heavy horizontal-scale need at this project's scale; Postgres is easier to justify and query for the dashboard.

### D5 — Docker Compose for the whole stack

Rubric explicitly cites reproducibility via Docker (5 marks) — treated as effectively required.

### D6 — Simulated clock: 1 simulated day = 5 real minutes

Streaming events emit every 3 seconds. State this explicitly and identically in the README and the report — an easy mark not to lose.

### D7 — Kafka: KRaft mode, not Zookeeper (`apache/kafka:4.1.2`)

**Correction made mid-build.** Original plan used Confluent images with Zookeeper — outdated. Kafka removed Zookeeper support entirely in 4.0 (April 2024). Switched to the official `apache/kafka` image running combined broker+controller KRaft mode, single node. No Zookeeper container exists in the stack at all.

Key env vars used (see `docker-compose.yml`):

- `KAFKA_PROCESS_ROLES=broker,controller`
- `KAFKA_LISTENERS` / `KAFKA_ADVERTISED_LISTENERS` split into `PLAINTEXT` (internal, port 9092), `CONTROLLER` (port 9093, internal only), `PLAINTEXT_HOST` (host-accessible, port 9094)
- Host-side scripts connect via `localhost:9094`; containers connect via `kafka:9092`

### D8 — PostgreSQL 18 (not 15)

Current stable major version. **Important gotcha discovered:** Postgres 18+ images changed their expected volume mount layout — must mount the volume at `/var/lib/postgresql` (not `/var/lib/postgresql/data` as in all earlier versions), since the image now manages a version-specific subdirectory internally for `pg_upgrade --link` compatibility. Mounting the old way crash-loops the container even on a fresh volume.

### D9 — Airflow 3.3.2 via `airflow standalone` (not split webserver/scheduler, not Celery)

Airflow 3.x restructured the architecture (webserver → api-server, plus dag-processor as a distinct component in full production layouts). For a 2-week single-node academic project, `airflow standalone` is still the officially documented way to run Airflow locally — it bundles scheduler, API server, triggerer and DAG processing into one process. Configured with `LocalExecutor` + Postgres backend (not the default SQLite) so it isn't limited to sequential execution. **State this explicitly in the report's trade-offs section**: a real production deployment would split these into separate services per Airflow's own docs, but that's out of scope here.

**Gotcha discovered:** Airflow 3.x defaults to a "Simple Auth Manager" instead of the old FAB username/password login. The `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` env vars do not reliably set the login — Airflow auto-generates its own admin password into a file inside the container (and regenerates it whenever the container is recreated). Retrieve it with:

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

**Gotcha discovered:** Airflow's first boot can race Postgres's own startup and crash instead of retrying (`connection to server at "postgres" ... Connection refused`). Fixed by adding a `healthcheck` (`pg_isready`) to the `postgres` service and changing `airflow`'s `depends_on` to `condition: service_healthy` instead of a plain list — see current `docker-compose.yml`.

### D10 — SQLAlchemy + Alembic instead of a raw `init_db.sql` script

**Correction made mid-build**, at the user's request, after the original raw-SQL init-mount approach kept failing on Windows (Docker silently mounts an empty directory instead of erroring when the source file path doesn't resolve — happened twice, once because the file was never actually created on disk, and generally fragile with the project's path containing spaces/parentheses).

Switched to:

- `storage/models.py` — SQLAlchemy 2.0 typed declarative models (`Mapped`/`mapped_column`, not the legacy `declarative_base()` style)
- `storage/migrations/` — Alembic migration environment, `DATABASE_URL` read from `.env`
- Schema created and versioned via `uv run alembic revision --autogenerate` + `uv run alembic upgrade head`

**Important scope note for later phases:** this only governs schema definition, migrations, and the FastAPI serving layer's reads. It does **not** replace direct psycopg2/JDBC writes from the Spark speed layer or Airflow batch job — ORM writes are too slow for streaming micro-batches. State this as an intentional, not accidental, split in the report.

The `airflow_meta` database (Airflow's own metadata DB, separate from the app's `ridehailing` DB) is created manually with one command rather than via an init script, since Alembic only manages schema within a single database:

```bash
docker compose exec postgres psql -U pipeline -d postgres -c "CREATE DATABASE airflow_meta;"
```

Needed once after any full volume wipe (`docker volume rm ..._pgdata`).

### D11 — Postgres host port remapped to 5433

User already runs a local Postgres on port 5432. Compose maps `5433:5432` (host:container) — container-internal port stays 5432, unaffected. Only host-side connection strings (`.env`'s `POSTGRES_PORT`, `DATABASE_URL`) use 5433; Airflow's and the API's internal connection strings use the Docker service name `postgres` with the implicit internal port 5432, unaffected by the host remap.

### D12 — Windows/Git Bash (MINGW64) quirks to remember for all future commands

- Any `docker compose exec <service> /absolute/path/...` command needs `MSYS_NO_PATHCONV=1` prefixed (or double the leading slash, `//path/...`), otherwise Git Bash rewrites the Unix path into a bogus Windows path before Docker sees it.
- PySpark should run **inside Docker**, not via local `uv run`, to avoid native Windows/JVM setup friction (`winutils.exe`, `HADOOP_HOME`).
- `grep` exits with code 1 and prints nothing when it finds no match — that is "no match," not an error.

### D13 — kafka-python over confluent-kafka for the producer scripts

kafka-python is pure-Python (no native/C dependency), which avoids Windows wheel-availability risk; confluent-kafka wraps librdkafka (C library) for better throughput, but at this project's scale (~15 simulated vehicles, one event per vehicle every 3-5s) throughput is not a constraint. Also already installed. Checked current state: kafka-python had a stagnant period historically but is actively maintained again (3.0.x line as of 2026), so this isn't reviving a dead library.

### D14 — Batch file output directory: storage/batch_incoming/

Chosen instead of a new top-level folder because storage/ is already mounted
into the airflow container, so the Phase 4 file-sensor DAG can watch
/opt/airflow/storage/batch_incoming/ with no docker-compose change needed.

### D15 — (unused)

Number skipped during Phase 3; intentionally left empty so later references (D16 onward) stay stable.

### D16 — Custom Spark image (processing/Dockerfile) instead of the bare apache/spark image

transformWithStateInPandas (stateful idle detection) needs pandas + pyarrow,
and the Postgres sinks need psycopg2 — none are bundled in the official
apache/spark:4.1.3-\*-python3-ubuntu image. Added a one-line Dockerfile that
installs them on top of the base image; docker-compose now `build`s this
instead of just pulling.

### D17 — Windowed metrics sink uses append mode + watermark, not upsert

Chose outputMode="append" with a 2-minute watermark over update-mode +
Postgres upsert. Append mode with a watermark guarantees each (window, zone)
row is emitted exactly once after the watermark passes the window's end, so
a plain INSERT is correct with no unique constraint or ON CONFLICT needed.
Trade-off: a window's rows appear roughly 2-3 minutes after the window ends
(about 7-8 minutes after it starts) rather than instantly — acceptable for a
5-minute dashboard window. The watermark only advances when newer events arrive,
so with the producer stopped the newest windows are never emitted.
(Caveat: `foreachBatch` is at-least-once, so a batch replayed after a crash could in
principle insert duplicate rows. Checked in Phase 5: no duplicates exist in 976 rows —
see D38; the API de-duplicates defensively anyway.)

### D18 — transformWithStateInPandas over applyInPandasWithState

Verified current docs: transformWithStateInPandas is the Spark 4.0+
replacement for the older applyInPandasWithState arbitrary-stateful API.
Used it for per-vehicle idle-duration tracking (needed for the alert
requirement) since it's the current, actively-documented pattern.

### D19 — approx_count_distinct instead of count(distinct ...) in the streaming aggregation

Structured Streaming does not support exact distinct aggregations at all
(not a performance choice — Spark raises AnalysisException). Switched
active_vehicles/idle_vehicles to approx_count_distinct, per Spark's own
error message. At 15 vehicles the approximation is effectively exact
(HyperLogLog is highly accurate at small cardinalities). (Note: the counts are still
per zone and overlap across zones — see D39.)

### D20 — Pin protobuf to exactly 6.33.0 in processing/Dockerfile

transformWithStateInPandas's Python↔JVM state-server RPC uses generated
Protobuf stub code. Installing protobuf unpinned resolved to an incompatible
version and failed silently — not an import error, but a hang
(`SocketTimeoutException: Timed out while waiting for the Python worker to
connect back`), which took far longer to diagnose than a normal version
conflict would have. Fetched Spark's own `dev/requirements.txt` for the
`v4.1.3` git tag, which pins `protobuf==6.33.0` under "Spark Connect
(required)" — used that exact pin rather than guessing a compatible version.

### D21 — Force RocksDBStateStoreProvider for stateful streaming queries

transformWithStateInPandas needs multiple state "column families" internally,
which the default HDFSBackedStateStoreProvider doesn't support
(`StateStoreMultipleColumnFamiliesNotSupportedException`). Confirmed via
Spark's own error text and Databricks' introductory blog post for the
transformWithState feature family. Added
`spark.sql.streaming.stateStore.providerClass =
org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` in
`build_spark()`.

**Gotcha discovered:** Structured Streaming bakes the state-store provider
class into each stateful query's checkpoint metadata on its first successful
run, and enforces that recorded value on every restart — silently
overriding the SparkSession config if it disagrees (only a WARN log line as
a clue: `Updating the value of conf 'spark.sql.streaming.stateStore.
providerClass' ... from 'RocksDBStateStoreProvider' to
'HDFSBackedStateStoreProvider'`). Any time this config changes, the
checkpoint directories for the affected stateful queries
(`storage/checkpoints/alerts`, `storage/checkpoints/realtime_fleet_metrics`)
must be deleted so the new config actually takes effect on a fresh
checkpoint. (But see D34: if you reset checkpoints, also clear `storage/raw_events`.)

### D22 — Give Kafka a persistent named volume

`docker-compose.yml`'s `kafka` service had no volume, so every
`docker compose down` destroyed every topic. On the next `up`, Spark's
readers would fail with `UnknownTopicOrPartitionException` until the
producer script ran again to auto-recreate the topic — and any existing
checkpoint still referencing offsets from the now-dead topic made it worse.
Added `kafka-data:/tmp/kraft-combined-logs` — the exact default `log.dirs`
path, verified against `apache/kafka`'s own `docker/server.properties` on
GitHub for the current image rather than assumed. Matches the persistence
already given to `postgres` (`pgdata`) and the Spark Ivy cache
(`spark-ivy-cache`). `docker compose down -v` still wipes it deliberately;
ordinary `down`/`up` no longer does.

### D23 — Parse event_time with Spark's built-in ISO-8601 parser, not a hand-written pattern

`read_parsed_events()` originally used
`F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")`. A
transcription error (the pattern lost its `'T'` literal at some point)
caused `CANNOT_PARSE_TIMESTAMP` on every event. Fixed by dropping the
explicit pattern entirely in favor of `F.to_timestamp("timestamp")` — Spark's
built-in ISO-8601 parser handles the `T` separator and the `+00:00` offset
natively, and tolerates a variable (or entirely absent) fractional-seconds
part. This also pre-empts a second latent bug the fixed-pattern approach
had: Python's `datetime.isoformat()` omits the fractional-seconds part
entirely when microseconds happen to be exactly 0, which would have broken
a strict 6-digit SSSSSS pattern intermittently and unpredictably.

---

## Phase 4 decisions (batch layer & orchestration)

### D24 — Timestamped batch filenames + atomic write; sim-day window derived from the filename

`vehicle_costs_dayNNN.csv` carried only a sim-day index that resets to 0 whenever
batch*generator restarts (overwriting earlier files), and raw Parquet is partitioned by real UTC
date with many sim days per date, so the index could not identify which slice of events belongs
to which CSV. Files are now `vehicle_costs*<UTC window end YYYYMMDDTHHMMSSZ>.csv`; the
reconciliation window is [end − SIM_DAY_DURATION_SECONDS, end). Files are written under a hidden
`.tmp` name and renamed (`os.replace`) so the sensor's glob never matches a half-written CSV.
Consequence: restarting the generator leaves a gap in coverage (no CSV for the time it was off).

### D25 — Earnings come from raw Parquet, not realtime_fleet_metrics

`realtime_fleet_metrics` is aggregated per zone, so per-vehicle earnings cannot be recovered from
it. The batch layer recomputes from raw events using the producer's completion contract
(status='idle' AND trip_id not null, fare final), de-duplicated on trip_id. This also gives the
report's Lambda argument its substance: the batch layer independently recomputes from
authoritative data instead of re-reading the speed layer's output.

### D26 — Batch job runs as pandas + pyarrow inside a custom Airflow image

Options were PySpark local mode in Airflow (Java + pyspark, heavy), DockerOperator via the
Docker socket (fiddly on Docker Desktop for Windows), or pandas/pyarrow in Airflow. Chose the
last: at ~15 vehicles per day-window the data is tiny and it is the most reliable on this setup.
Trade-off to state in the report: the batch layer is not Spark (D3 anticipated using it for both
layers), and a production deployment at real volume would run it on Spark. Image extended per
Airflow's docs (`orchestration/Dockerfile`): install as the airflow user, pin
apache-airflow==${AIRFLOW_VERSION}, use the image's own constraints file.

### D27 — vehicle_profitability extended; idempotent upsert; report_date is the sim-day end

Added trips_completed, cost_per_km, service_flag and a unique constraint on
(report_date, vehicle_id) via Alembic ("extend vehicle_profitability for batch layer"). Writes are
INSERT ... ON CONFLICT DO UPDATE, so a retried or re-run day overwrites instead of duplicating.
`report_date` holds the UTC end instant of the simulated day (many per real calendar date, naive
UTC), not a calendar date. net_profit = earnings − fuel − maintenance; is_profitable = net_profit > 0;
vehicles with no completed trips get earnings 0. CSV distance is treated as authoritative for
cost_per_km. Verified: re-processing the same windows reproduces identical rows.

### D28 — Schedule + FileSensor(reschedule, soft_fail) + archive-on-success

DAG runs every 5 minutes (matching the sim day), `max_active_runs=1`, `catchup=False`. FileSensor
(apache-airflow-providers-standard; the old core import path was removed in Airflow 3) waits for
`batch_incoming/vehicle_costs_*.csv`; reschedule mode frees the worker slot between pokes and
soft_fail turns "no file this interval" into a skipped run rather than a failure. Processed CSVs
are moved to storage/batch_processed/ only after the data-quality task passes, so "pending" is
visible on disk and a failed file is retried automatically on the next run. The fs_default
connection is defined via AIRFLOW_CONN_FS_DEFAULT in docker-compose so no manual UI step is needed.
Gotcha: with max_active_runs=1 a manually triggered run queues behind a running scheduled run —
this is why an early manual trigger sat in "Queued". (The earlier "one malformed CSV blocks later
files" limitation is removed by D32: files are now independent.)

### D29 — Raw Parquet is read through Spark's sink log (\_spark_metadata), not by directory listing

Spark's file sink treats a file as visible only if it appears in the sink log with action "add";
pandas/pyarrow ignore the log and could read a part file that is still being written.
committed_raw_files() parses the log (v1 header + JSON lines; `<id>` and `<id>.compact` files;
compact logs merge older logs, which Spark later deletes, so the union of all present logs is
used). (The fixed 90 s post-window wait that was originally part of this decision is superseded by
D31.) Limitation: it scans every committed file of the relevant date partitions each run, which is
fine at this volume.

### D30 — Data-quality gate before archiving

Fails the run if the day has zero rows, a row count different from the CSV's, or nulls in key
columns (vehicle_id, earnings, costs, distance, net_profit, is_profitable). cost_per_km may
legitimately be null when distance is 0, so it is excluded from the null check. The gate is
structural only: it cannot detect a window whose raw events were incompletely committed — that is
what D31 guarantees.

### D31 — Event-time readiness check replaces the fixed 90 s grace period

Spark micro-batches were observed taking 40–300 s, so a fixed wait after the window end gave no
completeness guarantee. (Re-running two windows later gave identical numbers, so no data had been
lost yet — but nothing prevented it.) New rule: a window is complete once a committed raw event has
event_time ≥ window_end + 6 s (STREAM_READY_MARGIN_SECONDS). Events arrive in event-time order on a
single Kafka partition, so everything inside the window is then in the sink; the margin also covers
skew if the topic is ever given more partitions. The wait polls every 10 s and fails with
TimeoutError after 540 s (CSV stays in batch_incoming and the next run retries) — completeness is
chosen over liveness. Observed: waits of ~14 s instead of ≥90 s.

### D32 — Process all pending CSVs per run via dynamic task mapping

One CSV arrives every 5 minutes and one run used to process only the oldest, so a backlog could never
drain. The DAG now maps wait_for_stream_ready → reconcile → check_data_quality →
archive_processed_file over every pending file (`.expand`), one mapped instance per CSV. A failure in
one file does not block the others, and idempotent upserts (D27) make retries and reprocessing safe.
Verified by re-queuing three already-processed CSVs: one run reprocessed and archived them with
identical results (the mapped-instance view itself was not screenshotted).

### D33 — Cost generator scaled to the compressed simulated day

The first cost ranges (60–320 km, 25–40 LKR/km, service 5,000–20,000) modelled a real working day, but
a 5-minute simulated day yields only ~1–2 trips per vehicle, so costs of ~2,000–10,000 against earnings of
~1,500 left almost every vehicle unprofitable (0–3 of 15 per window). New ranges: distance 10–45 km,
same 25–40 LKR/km, maintenance 0–400 (service day 1,500–4,000). Result: 4–5 of 15 profitable when trip
counts are healthy. Costs remain independent of the producer by design — they model an external,
authoritative source (fuel cards / workshop invoices). Rows produced before the change (report_dates
up to 2026-10-01 08:51:28, plus the 2026-09-30 row) use the old scale.

### D34 — Resetting Spark checkpoints requires also clearing storage/raw_events

Confirmed in Spark's FileStreamSink source: `addBatch` skips any batch whose id is at or below the latest
id recorded in the sink log ("Skipping already committed batch"). If checkpoints are deleted (batch ids
restart at 0) but `storage/raw_events/_spark_metadata` is kept, all new data is silently dropped. Rule:
`storage/checkpoints/*` and `storage/raw_events/` are reset together or not at all. Related: with an
existing checkpoint Spark resumes from its committed offsets and `startingOffsets=latest` is ignored; it
only applies to a first run with no checkpoint.

---

## Phase 5 decisions (serving layer)

Verified current patterns from official docs before building: FastAPI's Docker guide (exec-form `CMD`; `lifespan`
replaces the deprecated `on_event`), Pydantic v2 `ConfigDict(from_attributes=True)` + `model_validate`,
SQLAlchemy 2.0-style `select()` with `session.scalars()` and one `sessionmaker` per process, and uv's Docker guide
(pinned uv binary, `uv sync --locked --no-install-project` layering, `.venv` ignored). The host resolved
fastapi 0.141.1, pydantic 2.13.5, sqlalchemy 2.1.1 (the docs' "current" series; models are valid on 2.0 and 2.1),
starlette 1.7.0, uvicorn 0.54.0.

### D35 — Sync SQLAlchemy `Session` and plain `def` endpoints

The models and the rest of the stack use psycopg2, and FastAPI runs `def` endpoints in a threadpool. This avoids adding asyncpg or an async engine for a read-only service at this scale. One `sessionmaker` per process, one short-lived `Session` per request via a `yield` dependency, and `pool_pre_ping=True`.

### D36 — Run uvicorn directly, not `fastapi run`

The FastAPI docs say `fastapi run` uses uvicorn underneath, and the CLI needs `fastapi[standard]`. uvicorn is already a project dependency, so this adds no extras. The command is in exec form so shutdown signals and lifespan events work, and startup and shutdown use `lifespan`, not the deprecated `on_event`.

### D37 — `serving` dependency group, `--only-group`, root build context with an allow-list `.dockerignore`

The root `pyproject.toml` includes pyspark, pandas and kafka-python, so installing it whole would bloat the API image. The Dockerfile installs only the locked `serving` group (`uv sync --locked --only-group serving`). The build context must be the repo root because `storage/models.py` and `observability/` live there. The `.dockerignore` is an allow-list (excludes `.venv`, `.env`, `storage/raw_events`). Maintenance rule: a library the API needs must be added to both the main dependencies and the group.

### D38 — Duplicates in `realtime_fleet_metrics`: none observed, de-duplicated anyway

The check showed 976 rows and 976 distinct `(window_start, zone)` keys, and the detail query returned 0 rows. Because `foreachBatch` is at-least-once (D17), the API still reads with `DISTINCT ON (window_start, zone)`, latest `id` winning. This resolves the earlier open item.

### D39 — Fleet-level vehicle counts are not derived from `realtime_fleet_metrics`

Per-window `active_sum + idle_sum` across zones was 18–34 against a fleet of 15, because the counts are per zone and a vehicle is counted in each zone and status it appears in during the window. The API exposes them per zone only, labelled "vehicles seen," and takes fleet size from the batch layer. Trips and earnings are summed across zones.

### D40 — "Active alert" is time-based

(Superseded for pipeline alerts by D52; still applies to idle-vehicle alerts.) An alert is active if it is unresolved and was triggered within `active_minutes` (default 15). The database can't show whether a vehicle is still idle, and nothing sets `resolved=true` (363 alerts, 0 resolved at the time of the check). The dashboard states this. Real resolution needs a Spark change and belongs to Phase 6.

### D41 — The merged overview is approximate by design

`/fleet/overview` combines the latest batch day with the speed windows whose `window_end` is after the batch's `report_date`. Batch days end at arbitrary instants (10:10:47) and speed windows on 5-minute UTC boundaries (10:10:00), so there is no exact cutoff. The response carries both timestamps. For the report, this is the Lambda merge: the batch view is authoritative up to its cutoff, and the speed view fills the gap. (A speed layer window that ends before the batch cutoff, as observed at 11:05:00 vs 11:05:50, correctly contributes 0 windows.)

### D42 — Naive UTC timestamps are tagged UTC at the API boundary

A `UtcDatetime` type (an `AfterValidator`) makes every datetime serialize with `Z`. The dashboard shows UTC and offers browser-local time on hover. Money is returned as floats, because Pydantic serializes `Decimal` as a JSON string.

### D43 — The dashboard is one plain HTML/JS file served by FastAPI at `/`

No CDN and no build step, so it works offline for the demo and adds no container. It polls four endpoints every 5 s. Streamlit was rejected because it needs a second service and dependency.

### D44 — Freshness expectations corrected from measurement

Speed-layer `now − max(window_end)` swings between about 3 and 8 minutes (observed 5:56 and 7:39), so those values are normal, not stale. A window's rows land ~3 min after it closes and the next window follows 5 min later. The dashboard marks the speed layer stale above 600 s, and the Phase 6 alert threshold should be at least that. Batch lag `now − max(report_date)` ranges from about 1 to 6.5 minutes (dashboard stale mark: 720 s).

### D45 — API container operations: stdlib healthcheck, restart policy, no auth

The `python:3.11-slim` image has no curl, so the compose healthcheck calls `/health` with Python's `urllib` (every 15 s, 10 s start period). `restart: on-failure` is set on `api` (the spark service still has none, see open items). The API is read-only, single-origin and unauthenticated by design: it serves a demo on localhost, and the report should state that a production deployment would add authentication and rate limiting. First build on the development laptop took about 19 min (Docker Desktop layer export/unpack ~8 min, ~6.5 min of it `uv sync`); cached rebuilds are fast.

---

## Phase 6 decisions (observability)

### D46 — `pipeline_health` is current state: one row per component

The healthcheck upserts one row per component (`kafka`, `producer`, `speed_layer`, `batch_layer`, `airflow_dag`)
using `INSERT ... ON CONFLICT (component) DO UPDATE`. A unique constraint `uq_pipeline_health_component`
was added by Alembic migration `7c3e5a91b2d4` (rows from before the migration were deleted: they had no meaning
under the new contract). History is kept in `alerts`, not in `pipeline_health`. `last_heartbeat` only advances
when the status is `ok`, so it means "last known good".

### D47 — Health checking runs as its own Compose service; the producer signal is the Kafka end offset

`observability/healthcheck.py` runs as the `healthcheck` service (restart `unless-stopped`) every 15 s. "The
producer/stream stopped" is detected by the topic's end offset no longer advancing (kafka-python
`end_offsets`), not by inspecting processes or logs: it works regardless of where the producer runs (host
here) and is what the downstream layers actually depend on. Thresholds: `degraded` after 30 s without new
messages, `down` after 60 s (the simulated producer emits roughly every 3 s per vehicle, so 30 s is a large
safety margin). Speed/batch lag thresholds come from the measured normal ranges in D44 (speed degraded > 600 s,
batch degraded > 720 s, both down > 1200 s). Airflow thresholds: down if the latest finished run failed;
degraded if any run failed in the last 30 min or a run has been running ≥ 900 s.

### D48 — Airflow is checked through the REST API v2 with a fixed development login

Airflow 3.3.2 REST API v2 uses a JWT obtained from `POST /auth/token` (username + password) and sent as a
Bearer token (the checker re-logs in once on a 401). Simple Auth Manager generates a random password on every
container creation, so a fixed `admin:admin` is configured through
`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` and a mounted `simple_auth_passwords.json`. Simple Auth Manager is
documented as development/testing only; a production deployment would use a real auth manager and a secret.
Report limitation.

### D49 — Lag checks are gated on the producer, with a recovery grace period

If the producer is not `ok`, the speed and batch layers are reported `degraded` ("blocked by upstream") but
raise no alert of their own: their staleness is a consequence, and alerting on it would bury the root cause.
After the producer resumes, lag is not judged for 600 s (`RECOVERY_GRACE_SECONDS`), because the speed layer
needs about a window plus the watermark to catch up (D44). One open alert per component; it is opened on
`degraded`/`down`, escalated/downgraded when severity changes and resolved when the component returns to `ok`.

### D50 — The healthcheck image is pip-only with the code bind-mounted; one kafka-python finding

The image installs only `kafka-python==3.0.11` and `psycopg2-binary` with pip (no uv or lockfile for two
libraries) and mounts `./observability` read-only, so code changes need only a container restart. In
kafka-python 3.0.11 `api_version_auto_timeout_ms` is rejected (`KafkaConfigurationError`); the bootstrap
timeout is `bootstrap_timeout_ms` (default 30 s). Setting it to 5 s keeps a Kafka outage from stalling the
15 s check loop. `topics()` must be called before `partitions_for_topic()`, which can return `None` otherwise.

### D51 — `/health` reports pipeline health in the body and stays HTTP 200

`/health` now returns `status` (`ok`/`degraded`/`down`/`unknown`), `database` and a `pipeline` block with
the five components. HTTP 503 is returned only when the database is unreachable. A degraded or down pipeline
returns 200, otherwise the API container's own Docker healthcheck would mark the API unhealthy because of an
upstream problem the API cannot fix. If `pipeline_health` is empty or its newest `checked_at` is older than
60 s, the pipeline status is `unknown` ("healthcheck not reporting"), so a dead checker cannot look healthy.
The dashboard shows a header chip and a Pipeline health table.

### D52 — Alert "active" is now type-aware (supersedes the time-only rule in D40 for pipeline alerts)

Idle-vehicle alerts stay time-based (unresolved and fired within `active_minutes`, 15 by default) because
the Spark job never resolves them. `pipeline_health` alerts are active for as long as they are unresolved,
because the healthcheck resolves them itself when the component recovers.

### D53 — Not built, recorded as limitations

(a) A log error-rate check was planned (per-component log files plus an error-rate threshold) and is not
implemented; component failures are detected from their effects (Kafka offset, data lag, DAG run state)
instead. (b) Idle-vehicle alerts are still never resolved by the speed layer (D40 still applies to them);
fixing it needs an `idle_resolved` row from the stateful processor and a Spark restart with checkpoint
compatibility risk. (c) The healthcheck itself is a single process; its failure is made visible by the 60 s
staleness rule in D51 rather than by a second watcher.

### D54 — Batch layer cannot distinguish "no trips" from "the stream was down"

The batch readiness check (D31) passes once later events exist in the raw Parquet, so a day with a stream
outage produces a valid-looking row with zero or low trips. The producer-offset check (D47) and its alerts are
what flag the outage; the batch data for that day should be treated as incomplete. Report limitation.

### D55 — Request IDs for correlation, not distributed tracing

Every API request carries an `X-Request-ID`: a caller-supplied value is reused, otherwise a 16-character hex id is
generated. It is held in a `ContextVar`, attached to every log line written while the request is handled (via a
logging filter), returned in the response header, and returned in the body of a 500 so a user can quote it and an
operator can find the logged traceback. Between pipeline stages, records are correlated by `trip_id`, `vehicle_id` and
event time. Not built: spans or trace propagation across Kafka and Spark (OpenTelemetry), which would need message
headers and instrumentation of the Spark job. Documented as a limitation.

### D56 — `/metrics` in Prometheus text format, without a Prometheus server

`GET /metrics` exposes request counters and latency (labelled by route template, so ids do not create unbounded
labels), per-component status from `pipeline_health`, layer lag in seconds and the number of open alerts, in the
Prometheus text exposition format (`text/plain; version=0.0.4`). No Prometheus or Grafana containers were added: the
format makes the endpoint scrapeable later, and the dashboard and `/health` already cover the human view within the
project's time budget.

---

**Phase 2 & 3 exit criteria — verified 2026-09-30:**

- `realtime_fleet_metrics`: populated, up to 9 zones per 5-minute window, plausible
  vehicle counts and earnings (e.g. zone Z4: 3 trips, 4587.73 LKR total ≈
  1529/trip, within the producer's 300-2500 fare range).
- `alerts`: idle-vehicle alerts firing correctly after the 10-minute
  IDLE_ALERT_THRESHOLD_MINUTES, one alert per idle episode per vehicle (no
  duplicate spam), resolved=false on insert.
- `storage/raw_events/event_date=2026-09-30/`: 23 .parquet part-files
  (5-93 KB each) plus matching .crc files and `_spark_metadata/`, confirming
  the raw-storage sink is writing real data.

**Phase 4 exit criteria — verified 2026-10-01:**

- Host-side run (window ending 2026-09-30 17:24:12): 118,095 events read, 35 completed trips, 15 CSV rows,
  total earnings 48,165.73; net-profit arithmetic spot-checked correct.
- Airflow DAG v1 manual run: all tasks green, CSV archived. DAG v2 (readiness check + mapped tasks):
  nine consecutive scheduled runs green (2026-10-01 09:00–09:25 UTC), readiness wait ~14 s, run duration
  ~21 s–1:27 instead of ~4 min.
- Idempotency: re-running the same windows (CLI and via DAG) reproduces identical rows (25/9/4 trips for the
  07:31:28 / 07:36:28 / 07:41:28 windows).
- DB counts cross-checked against raw per-minute trip completions for four windows (within partial-minute
  boundary effects).
- New cost scale: 5, 4 and 1 profitable vehicles of 15 for windows 09:00:47, 09:05:47, 09:10:47 (tracks trip
  counts of 19, 16, 5).

**Phase 5 exit criteria — verified 2026-10-01:**

- Host run and container run both serve all endpoints; `api` container reports `healthy`; container logs are JSON
  with request durations of 1–3 ms.
- Cross-check against Postgres: speed window 10:20–10:25 = 21 trips / 26,573.34 LKR in API and DB; batch day
  ended 10:20:47 = 11 trips / 11,144.85 LKR / 2 profitable in API and DB; net-profit arithmetic checked
  (11,144.85 − 11,109.58 − 4,496.86 = −4,461.59).
- `/health` ok; unknown vehicle returns 404; `/fleet/overview` returns batch (day ended 11:05:50, 26 trips),
  latest speed window (11:00–11:05, 23 trips), `speed_since_batch` = 0 windows (correct, the window ends before
  the batch cutoff) and freshness; `/alerts` returned 8 active of 374 total; dashboard at
  `http://localhost:8000/` verified in a browser by the user.
- Measured, not assumed: no duplicate (window_start, zone) rows (976/976, D38); speed-layer lag 3–8 min is normal
  (D44); per-zone vehicle counts are not additive (D39).

**Phase 6 exit criteria — verified:**

- Producer stopped for about 3 minutes (20 Oct 2026): `producer` went `degraded` after 30 s (warning alert
  opened) and `down` after 60 s (escalated to critical); `speed_layer`/`batch_layer` went `degraded`
  "blocked by upstream" with no alerts of their own; when the producer restarted it returned to `ok` and the
  alert was resolved.
- Airflow failure (16 Nov 2026): with `processing/batch_reconciliation.py` deliberately broken, run
  `scheduled__2026-11-16T08:55:00+00:00` failed at 08:56:50 UTC; the healthcheck logged `airflow_dag ok -> down`
  and opened critical alert id 307 at 08:56:51 UTC; `/health` reported `status: down` with
  `airflow_dag down "latest finished run failed"`. The script was reverted afterwards (`git diff` clean).
- `/health` after deployment: all five components `ok`, database `ok`; dashboard shows the pipeline chip
  and table.
- Producer outage re-run (16 Nov 2026): alert 335 opened `critical` ("no new Kafka messages for 60 s") and was resolved
  after recovery.
- `/metrics` and request IDs verified on the running stack (16 Nov 2026): `curl -i -H "X-Request-ID: demo-001"
  localhost:8000/health` returned `x-request-id: demo-001`, and the API's JSON log line carried `"request_id": "demo-001"`;
  `/metrics` returned request counters, latency sums and per-component status in Prometheus text format.
- `uv run pytest tests -v`: 22 passed (health-check thresholds, Airflow client, JSON logging).
- Measured: speed-layer lag observed 246-471 s and batch-layer lag 109-544 s in the healthy state, inside the
  600 s / 720 s thresholds.


**Observation (not a decision):** trip completions arrive in waves with a period of roughly 15–18 minutes,
likely because all simulated vehicles start in sync (inference — producer code unchanged). Per-window trip counts
therefore vary widely early in a run (4–44 trips per window observed), and the DB counts match the raw events.
Individual vehicles can also show several consecutive zero-trip days (e.g. V012 after 5 trips earlier), which is
plausible given the producer's stuck-idle vehicles and the waves.
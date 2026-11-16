# Decisions Log

Running record of every non-trivial decision made so far, with reasoning, so the "why" survives a chat handoff and doubles as report material for the tech-stack-justification section.

**Numbering:** next free number is **D46**. D15 is intentionally unused (skipped during Phase 3; kept so existing references stay stable).

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

An alert is active if it is unresolved and was triggered within `active_minutes` (default 15). The database can't show whether a vehicle is still idle, and nothing sets `resolved=true` (363 alerts, 0 resolved at the time of the check). The dashboard states this. Real resolution needs a Spark change and belongs to Phase 6.

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

**Observation (not a decision):** trip completions arrive in waves with a period of roughly 15–18 minutes,
likely because all simulated vehicles start in sync (inference — producer code unchanged). Per-window trip counts
therefore vary widely early in a run (4–44 trips per window observed), and the DB counts match the raw events.
Individual vehicles can also show several consecutive zero-trip days (e.g. V012 after 5 trips earlier), which is
plausible given the producer's stuck-idle vehicles and the waves.

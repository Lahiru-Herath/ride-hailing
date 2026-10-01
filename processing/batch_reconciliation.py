"""
processing/batch_reconciliation.py

Batch layer (Lambda): once per simulated day, recompute every vehicle's
profitability from authoritative data and write it to Postgres.

Inputs
  - one daily cost CSV from storage/batch_incoming/
    (vehicle_id, fuel_cost, maintenance_cost, distance_covered_km, service_flag)
  - raw trip events (Parquet written by the Spark speed layer) for the same
    simulated day: events with event_time in [window_end - SIM_DAY, window_end),
    where window_end is parsed from the CSV filename (see D24).

Earnings = sum of the finalized `fare` of each completed trip in the window.
A trip is completed on the event with status='idle' AND non-null trip_id
(producer contract, see sources/streaming_producer.py). Trips are de-duplicated
on trip_id. Vehicles with no completed trip get earnings 0 (LEFT JOIN from the CSV).

Output: one row per vehicle in `vehicle_profitability`, upserted on
(report_date, vehicle_id), so re-running a day is idempotent (D27).

Completeness (D31): before reading, the job waits until the speed layer has committed
an event whose event_time is past the end of the window (plus a small margin). Events
arrive in event-time order, so everything inside the window is then in the sink.

Runs inside the Airflow container (orchestration/dags/daily_reconciliation_dag.py) but is
also runnable from the host for debugging:
    uv run python processing/batch_reconciliation.py storage/batch_incoming/vehicle_costs_<stamp>.csv [--wait-ready] [--archive]
"""

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

sys.path.append(str(Path(__file__).resolve().parent.parent))
from observability.logging_config import get_logger

try:  # python-dotenv is on the host; in the Airflow container config comes from docker-compose
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

logger = get_logger("batch_reconciliation")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STORAGE_ROOT = Path(os.environ.get("STORAGE_ROOT", PROJECT_ROOT / "storage"))
RAW_EVENTS_DIR = STORAGE_ROOT / "raw_events"
BATCH_INCOMING_DIR = STORAGE_ROOT / "batch_incoming"
BATCH_PROCESSED_DIR = STORAGE_ROOT / "batch_processed"

SIM_DAY_DURATION_SECONDS = int(float(os.environ.get("SIM_DAY_DURATION_SECONDS", "300")))

# Readiness check (D31). The window counts as complete once an event at least MARGIN seconds
# past window_end is committed. MARGIN covers a couple of producer ticks and any
# cross-partition skew if the topic is ever given more than one partition.
STREAM_READY_MARGIN_SECONDS = int(float(os.environ.get("STREAM_READY_MARGIN_SECONDS", "6")))
STREAM_READY_POLL_SECONDS = int(float(os.environ.get("STREAM_READY_POLL_SECONDS", "10")))
STREAM_READY_TIMEOUT_SECONDS = int(float(os.environ.get("STREAM_READY_TIMEOUT_SECONDS", "540")))

FILENAME_RE = re.compile(r"^vehicle_costs_(\d{8}T\d{6}Z)\.csv$")
SINK_LOG_FILE_RE = re.compile(r"\d+(\.compact)?")  # <batchId> or <batchId>.compact

COST_COLUMNS = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered_km", "service_flag"]
TRIP_COLUMNS = ["trip_id", "vehicle_id", "status", "fare", "event_time"]
DB_COLUMNS = [
    "vehicle_id",
    "total_earnings",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered_km",
    "net_profit",
    "is_profitable",
    "trips_completed",
    "cost_per_km",
    "service_flag",
]

UPSERT_SQL = """
    INSERT INTO vehicle_profitability (
        report_date, vehicle_id, total_earnings, fuel_cost, maintenance_cost,
        distance_covered_km, net_profit, is_profitable, trips_completed, cost_per_km, service_flag
    ) VALUES %s
    ON CONFLICT (report_date, vehicle_id) DO UPDATE SET
        total_earnings = EXCLUDED.total_earnings,
        fuel_cost = EXCLUDED.fuel_cost,
        maintenance_cost = EXCLUDED.maintenance_cost,
        distance_covered_km = EXCLUDED.distance_covered_km,
        net_profit = EXCLUDED.net_profit,
        is_profitable = EXCLUDED.is_profitable,
        trips_completed = EXCLUDED.trips_completed,
        cost_per_km = EXCLUDED.cost_per_km,
        service_flag = EXCLUDED.service_flag
"""


# ---------------------------------------------------------------------------
# File discovery / simulated-day window
# ---------------------------------------------------------------------------

def list_pending_files() -> list[Path]:
    """Unprocessed cost CSVs, oldest first (timestamped names sort chronologically)."""
    return sorted(
        p for p in BATCH_INCOMING_DIR.glob("vehicle_costs_*.csv") if FILENAME_RE.match(p.name)
    )


def window_from_filename(csv_path: str | Path) -> tuple[datetime, datetime]:
    """Return (window_start, window_end), both tz-aware UTC, for a cost CSV."""
    match = FILENAME_RE.match(Path(csv_path).name)
    if not match:
        raise ValueError(f"Unexpected cost CSV filename: {Path(csv_path).name}")
    window_end = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return window_end - timedelta(seconds=SIM_DAY_DURATION_SECONDS), window_end


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def load_costs(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, dtype={"vehicle_id": str})
    missing = set(COST_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Cost CSV is missing columns: {sorted(missing)}")
    if df.empty:
        raise ValueError("Cost CSV has no rows")
    if df["vehicle_id"].duplicated().any():
        raise ValueError("Cost CSV has duplicate vehicle_id rows")
    df = df[COST_COLUMNS].copy()
    df["service_flag"] = df["service_flag"].map(lambda v: str(v).strip().lower() == "true")
    return df


def _relative_raw_path(sink_log_path: str) -> str:
    """'file:///opt/spark-apps/storage/raw_events/event_date=.../part-x.parquet' -> 'event_date=.../part-x.parquet'."""
    path = unquote(urlparse(sink_log_path).path)
    marker = "/raw_events/"
    idx = path.find(marker)
    if idx == -1:
        raise ValueError(f"Sink log path is not under raw_events/: {sink_log_path}")
    return path[idx + len(marker):]


def committed_raw_files(event_dates: set[str]) -> list[Path]:
    """
    Parquet files the Spark file sink has COMMITTED, restricted to the given event_date partitions.

    Reading the directory directly (pandas/pyarrow) would ignore _spark_metadata and could hit a
    part file that is still being written. The sink log is the source of truth: a file is visible
    only if it appears in the log with action 'add'. Compact logs merge older batch logs, so the
    union over every log file present is the visible set.
    """
    log_dir = RAW_EVENTS_DIR / "_spark_metadata"
    if not log_dir.is_dir():
        raise FileNotFoundError(
            f"Spark sink log not found at {log_dir} - has the speed layer written any raw events yet?"
        )

    added: set[str] = set()
    deleted: set[str] = set()
    for log_file in log_dir.iterdir():
        if not (log_file.is_file() and SINK_LOG_FILE_RE.fullmatch(log_file.name)):
            continue
        with log_file.open(encoding="utf-8") as f:
            if f.readline().strip() != "v1":
                raise ValueError(f"Unexpected sink-log version in {log_file}")
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if entry.get("isDir"):
                    continue
                rel = _relative_raw_path(entry["path"])
                (added if entry.get("action") == "add" else deleted).add(rel)

    wanted = {f"event_date={d}" for d in event_dates}
    return [
        RAW_EVENTS_DIR / rel
        for rel in sorted(added - deleted)
        if rel.split("/", 1)[0] in wanted
    ]


def is_stream_ready(window_end: datetime) -> tuple[bool, datetime | None]:
    """
    True once a committed event has event_time >= window_end + margin (D31).

    Returns (ready, newest committed event_time seen). Files are scanned newest-first so the
    common case (ready) costs one small column read.
    """
    target = window_end + timedelta(seconds=STREAM_READY_MARGIN_SECONDS)
    try:
        files = committed_raw_files({window_end.strftime("%Y-%m-%d"), target.strftime("%Y-%m-%d")})
    except FileNotFoundError:
        return False, None

    def _mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    newest: datetime | None = None
    for path in sorted(files, key=_mtime, reverse=True):
        try:
            times = pd.read_parquet(path, columns=["event_time"])["event_time"]
        except (FileNotFoundError, OSError):
            continue
        if times.empty:
            continue
        latest = pd.to_datetime(times, utc=True).max().to_pydatetime()
        if newest is None or latest > newest:
            newest = latest
        if newest >= target:
            return True, newest
    return False, newest


def wait_until_stream_ready(
    csv_path: str | Path,
    timeout_seconds: int | None = None,
    poll_seconds: int | None = None,
) -> dict:
    """Block until the day's raw events are fully committed; raise TimeoutError otherwise."""
    timeout = STREAM_READY_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
    poll = STREAM_READY_POLL_SECONDS if poll_seconds is None else poll_seconds
    _, window_end = window_from_filename(csv_path)
    started = time.monotonic()

    while True:
        ready, newest = is_stream_ready(window_end)
        waited = round(time.monotonic() - started, 1)
        info = {
            "csv_file": Path(csv_path).name,
            "window_end": window_end.isoformat(),
            "newest_event_time": newest.isoformat() if newest else None,
            "waited_seconds": waited,
        }
        if ready:
            logger.info("stream_ready", extra=info)
            return info
        if waited >= timeout:
            logger.error("stream_not_ready", extra={**info, "timeout_seconds": timeout})
            raise TimeoutError(
                f"Speed layer has not committed events past {window_end.isoformat()} "
                f"after {timeout}s (newest committed event: {info['newest_event_time']}). "
                "Is the producer / Spark running?"
            )
        logger.info("waiting_for_stream", extra=info)
        time.sleep(poll)


def load_completed_trips(window_start: datetime, window_end: datetime) -> pd.DataFrame:
    """Completed trips (one row per trip_id) with event_time in [window_start, window_end)."""
    dates = {window_start.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")}  # UTC partitions
    files = committed_raw_files(dates)

    frames = [pd.read_parquet(f, columns=TRIP_COLUMNS) for f in files]
    frames = [f for f in frames if not f.empty]
    if not frames:
        logger.warning(
            "no_raw_events_found",
            extra={"window_start": window_start.isoformat(), "window_end": window_end.isoformat()},
        )
        return pd.DataFrame(columns=TRIP_COLUMNS)

    events = pd.concat(frames, ignore_index=True)
    events["event_time"] = pd.to_datetime(events["event_time"], utc=True)

    completed = events[
        (events["status"] == "idle")
        & events["trip_id"].notna()
        & (events["event_time"] >= window_start)
        & (events["event_time"] < window_end)
    ]
    completed = completed.sort_values("event_time").drop_duplicates(subset="trip_id", keep="last")

    logger.info(
        "raw_events_loaded",
        extra={
            "raw_files": len(files),
            "events_read": len(events),
            "completed_trips": len(completed),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        },
    )
    return completed


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def compute_profitability(costs: pd.DataFrame, completed: pd.DataFrame) -> pd.DataFrame:
    per_vehicle = (
        completed.groupby("vehicle_id")
        .agg(trips_completed=("trip_id", "nunique"), total_earnings=("fare", "sum"))
        .reset_index()
    )

    unknown = set(per_vehicle["vehicle_id"]) - set(costs["vehicle_id"])
    if unknown:
        logger.warning("earnings_for_vehicles_not_in_cost_csv", extra={"vehicles": sorted(unknown)})

    df = costs.merge(per_vehicle, on="vehicle_id", how="left")
    df["trips_completed"] = pd.to_numeric(df["trips_completed"]).fillna(0).astype(int)
    df["total_earnings"] = pd.to_numeric(df["total_earnings"]).fillna(0.0).astype(float)

    for col in ("total_earnings", "fuel_cost", "maintenance_cost"):
        df[col] = df[col].astype(float).round(2)
    df["net_profit"] = (df["total_earnings"] - df["fuel_cost"] - df["maintenance_cost"]).round(2)
    df["is_profitable"] = df["net_profit"] > 0

    distance = df["distance_covered_km"].where(df["distance_covered_km"] > 0)  # avoid divide-by-zero
    df["cost_per_km"] = ((df["fuel_cost"] + df["maintenance_cost"]) / distance).round(4)
    return df


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _native(value):
    """numpy scalar / NaN -> plain Python value / None so psycopg2 can adapt it."""
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def write_profitability(result: pd.DataFrame, report_date: datetime) -> int:
    rows = [
        (report_date, *(_native(v) for v in record))
        for record in result[DB_COLUMNS].itertuples(index=False, name=None)
    ]
    conn = connect()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


# ---------------------------------------------------------------------------
# Orchestration entry points (called by the Airflow DAG and the CLI)
# ---------------------------------------------------------------------------

def reconcile_file(csv_path: str | Path) -> dict:
    window_start, window_end = window_from_filename(csv_path)
    report_date = window_end.replace(tzinfo=None)  # naive UTC, matches the DateTime column

    costs = load_costs(csv_path)
    completed = load_completed_trips(window_start, window_end)
    result = compute_profitability(costs, completed)
    write_profitability(result, report_date)

    summary = {
        "csv_file": Path(csv_path).name,
        "report_date": report_date.isoformat(),
        "csv_rows": int(len(costs)),
        "trips_counted": int(result["trips_completed"].sum()),
        "profitable_vehicles": int(result["is_profitable"].sum()),
        "unprofitable_vehicles": int((~result["is_profitable"]).sum()),
        "total_earnings": float(round(result["total_earnings"].sum(), 2)),
    }
    logger.info("reconciliation_complete", extra=summary)
    return summary


def check_quality(report_date_iso: str, expected_rows: int) -> dict:
    """Data-quality gate: raises ValueError if the day's rows are missing or have nulls in key columns."""
    report_date = datetime.fromisoformat(report_date_iso)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    count(*),
                    count(*) FILTER (
                        WHERE vehicle_id IS NULL OR total_earnings IS NULL OR fuel_cost IS NULL
                           OR maintenance_cost IS NULL OR distance_covered_km IS NULL
                           OR net_profit IS NULL OR is_profitable IS NULL
                    )
                FROM vehicle_profitability
                WHERE report_date = %s
                """,
                (report_date,),
            )
            row_count, null_rows = cur.fetchone()
    finally:
        conn.close()

    problems = []
    if row_count == 0:
        problems.append("no rows written")
    elif row_count != expected_rows:
        problems.append(f"expected {expected_rows} rows, found {row_count}")
    if null_rows:
        problems.append(f"{null_rows} rows have nulls in key columns")

    result = {"report_date": report_date_iso, "row_count": row_count, "null_rows": null_rows}
    if problems:
        logger.error("quality_check_failed", extra={**result, "problems": problems})
        raise ValueError("; ".join(problems))
    logger.info("quality_check_passed", extra=result)
    return result


def archive_batch_file(csv_path: str | Path) -> str:
    """Move a fully processed CSV out of batch_incoming/ so the sensor never sees it again."""
    BATCH_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    destination = BATCH_PROCESSED_DIR / Path(csv_path).name
    os.replace(csv_path, destination)
    logger.info("batch_file_archived", extra={"file": str(destination)})
    return str(destination)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile one daily cost CSV against raw trip events.")
    parser.add_argument("csv_file", help="path to storage/batch_incoming/vehicle_costs_<stamp>.csv")
    parser.add_argument(
        "--wait-ready",
        action="store_true",
        help="first wait until the speed layer has committed events past the end of the window",
    )
    parser.add_argument(
        "--archive",
        action="store_true",
        help="after the quality check passes, move the CSV to storage/batch_processed/",
    )
    args = parser.parse_args()

    if args.wait_ready:
        wait_until_stream_ready(args.csv_file)
    summary = reconcile_file(args.csv_file)
    check_quality(summary["report_date"], summary["csv_rows"])
    if args.archive:
        archive_batch_file(args.csv_file)


if __name__ == "__main__":
    main()
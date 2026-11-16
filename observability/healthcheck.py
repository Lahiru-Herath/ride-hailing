"""
observability/healthcheck.py

Pipeline health checker (Phase 6). Runs as the `healthcheck` Compose service and every
HEALTH_CHECK_INTERVAL_SECONDS evaluates:

  kafka        broker reachable and topic exists
  producer     the topic's end offset is still advancing (the primary "stream stopped" signal, D44)
  speed_layer  lag of max(window_end) in realtime_fleet_metrics (judged only while the producer is healthy)
  batch_layer  lag of max(report_date) in vehicle_profitability (same gating)
  airflow_dag  latest finished DAG run state, recent failures, stuck runs (Airflow REST API v2, JWT)

Each result is upserted into pipeline_health (one row per component). A degraded/down result raises a
row in `alerts` (alert_type='pipeline_health'); it is resolved when the component returns to ok.
The evaluate_* functions are pure so they can be unit-tested without Kafka, Postgres or Airflow.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
from kafka import KafkaConsumer, TopicPartition

sys.path.append(str(Path(__file__).resolve().parent.parent))
from observability.logging_config import get_logger

log = get_logger("healthcheck")


def _f(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# --- thresholds (D44 for the lag values; D47 for the producer/airflow values) ---
CHECK_INTERVAL_S = _f("HEALTH_CHECK_INTERVAL_SECONDS", 15)
PRODUCER_DEGRADED_S = _f("PRODUCER_DEGRADED_SECONDS", 30)
PRODUCER_DOWN_S = _f("PRODUCER_DOWN_SECONDS", 60)
SPEED_STALE_S = _f("SPEED_STALE_SECONDS", 600)
SPEED_DOWN_S = _f("SPEED_DOWN_SECONDS", 1200)
BATCH_STALE_S = _f("BATCH_STALE_SECONDS", 720)
BATCH_DOWN_S = _f("BATCH_DOWN_SECONDS", 1200)
RECOVERY_GRACE_S = _f("RECOVERY_GRACE_SECONDS", 600)  # lag is not judged this long after the producer resumes
AIRFLOW_FAIL_WINDOW_S = _f("AIRFLOW_FAIL_WINDOW_SECONDS", 1800)
AIRFLOW_STUCK_S = _f("AIRFLOW_STUCK_SECONDS", 900)
AIRFLOW_UNREACHABLE_STRIKES = int(_f("AIRFLOW_UNREACHABLE_STRIKES", 3))

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_TRIP_EVENTS", "trip_events")
KAFKA_TIMEOUT_MS = int(_f("KAFKA_TIMEOUT_MS", 5000))
AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://airflow:8080").rstrip("/")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
DAG_ID = os.environ.get("AIRFLOW_DAG_ID", "daily_reconciliation")

LONG_AGO = datetime(2000, 1, 1)


@dataclass
class Result:
    component: str
    status: str  # ok | degraded | down | unknown
    message: str
    details: dict = field(default_factory=dict)
    alert: bool = True  # False: report the status but do not raise an alert (e.g. blocked by upstream)


def utcnow() -> datetime:
    """Naive UTC, comparable with the database's timestamp-without-time-zone columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ------------------------------------------------------------------ pure evaluators
class OffsetTracker:
    """Remembers when the topic's end offset last changed."""

    def __init__(self, now: datetime):
        self.offset: int | None = None
        self.changed_at = now

    def observe(self, offset: int, now: datetime) -> float:
        if self.offset is None or offset != self.offset:  # any change, including a topic reset
            self.changed_at = now
        self.offset = offset
        return (now - self.changed_at).total_seconds()


def evaluate_producer(stalled_s: float, end_offset: int) -> Result:
    details = {"end_offset": end_offset, "stalled_seconds": round(stalled_s)}
    if stalled_s >= PRODUCER_DOWN_S:
        return Result("producer", "down", f"no new Kafka messages for {round(stalled_s)} s", details)
    if stalled_s >= PRODUCER_DEGRADED_S:
        return Result("producer", "degraded", f"no new Kafka messages for {round(stalled_s)} s", details)
    return Result("producer", "ok", "events flowing", details)


def evaluate_lag(component: str, newest: datetime | None, now: datetime, stale_s: float, down_s: float,
                 producer_status: str, producer_ok_since: datetime | None) -> Result:
    if newest is None:
        return Result(component, "unknown", "no data yet", {}, alert=False)
    lag = (now - newest).total_seconds()
    details = {"lag_seconds": round(lag), "data_through": newest.isoformat()}
    if producer_status != "ok":
        return Result(component, "degraded", "blocked by upstream: producer", {**details, "blocked_by": "producer"}, alert=False)
    if producer_ok_since is not None and (now - producer_ok_since).total_seconds() < RECOVERY_GRACE_S:
        return Result(component, "degraded", "recovering after producer outage", {**details, "recovering": True}, alert=False)
    if lag >= down_s:
        return Result(component, "down", f"data is {round(lag)} s behind", details)
    if lag >= stale_s:
        return Result(component, "degraded", f"data is {round(lag)} s behind", details)
    return Result(component, "ok", f"data is {round(lag)} s behind (normal)", details)


def _parse(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def evaluate_airflow(runs: list[dict], now: datetime) -> Result:
    """runs: DAG run objects from GET /api/v2/dags/{id}/dagRuns (any order)."""
    if not runs:
        return Result("airflow_dag", "unknown", "no DAG runs yet (DAG paused or just started?)", {}, alert=False)
    runs = sorted(runs, key=lambda r: _parse(r.get("run_after")) or LONG_AGO, reverse=True)
    finished = [r for r in runs if r.get("state") in ("success", "failed")]
    running = [r for r in runs if r.get("state") == "running"]
    details = {"latest_run": runs[0].get("dag_run_id"), "latest_state": runs[0].get("state")}

    if finished and finished[0]["state"] == "failed":
        return Result("airflow_dag", "down", f"latest finished run failed: {finished[0].get('dag_run_id')}",
                      {**details, "failed_run": finished[0].get("dag_run_id")})
    recent_failed = [r for r in finished if r["state"] == "failed"
                     and (_parse(r.get("end_date")) or LONG_AGO) >= now - timedelta(seconds=AIRFLOW_FAIL_WINDOW_S)]
    if recent_failed:
        return Result("airflow_dag", "degraded", f"a run failed in the last {round(AIRFLOW_FAIL_WINDOW_S / 60)} min",
                      {**details, "failed_run": recent_failed[0].get("dag_run_id")})
    for r in running:
        started = _parse(r.get("start_date"))
        if started and (now - started).total_seconds() >= AIRFLOW_STUCK_S:
            return Result("airflow_dag", "degraded", f"run {r.get('dag_run_id')} running for {round((now - started).total_seconds())} s",
                          {**details, "stuck_run": r.get("dag_run_id")})
    return Result("airflow_dag", "ok", "recent runs succeeded", details)


# ------------------------------------------------------------------ I/O: Kafka
def read_end_offset() -> tuple[int, int]:
    """Sum of end offsets over the topic's partitions, and the partition count."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=None,
        enable_auto_commit=False,
        request_timeout_ms=KAFKA_TIMEOUT_MS + 5000,
        session_timeout_ms=KAFKA_TIMEOUT_MS,
        heartbeat_interval_ms=max(KAFKA_TIMEOUT_MS // 3, 1000),
        bootstrap_timeout_ms=KAFKA_TIMEOUT_MS,  # default 30 s would stall the check loop when Kafka is down
    )
    try:
        consumer.topics()  # forces a metadata refresh; partitions_for_topic can return None before it
        parts = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not parts:
            raise RuntimeError(f"topic {KAFKA_TOPIC!r} not found")
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in sorted(parts)]
        ends = consumer.end_offsets(tps, timeout_ms=KAFKA_TIMEOUT_MS)
        return sum(ends.values()), len(tps)
    finally:
        consumer.close()


# ------------------------------------------------------------------ I/O: Airflow REST API v2
class AirflowClient:
    def __init__(self):
        self.token: str | None = None

    def _login(self) -> None:
        req = urllib.request.Request(
            f"{AIRFLOW_URL}/auth/token",
            data=json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            self.token = json.load(resp)["access_token"]

    def dag_runs(self, limit: int = 20) -> list[dict]:
        url = f"{AIRFLOW_URL}/api/v2/dags/{DAG_ID}/dagRuns?limit={limit}&order_by=-run_after"
        for attempt in (1, 2):
            if self.token is None:
                self._login()
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}"})
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return json.load(resp)["dag_runs"]
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and attempt == 1:
                    self.token = None  # expired token: log in again once
                    continue
                raise
        return []


# ------------------------------------------------------------------ I/O: Postgres
def connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "ridehailing"),
        user=os.environ.get("POSTGRES_USER", "pipeline"),
        password=os.environ.get("POSTGRES_PASSWORD", "pipeline"),
        connect_timeout=5,
    )


UPSERT_SQL = """
INSERT INTO pipeline_health (component, status, last_heartbeat, details, checked_at)
VALUES (%(component)s, %(status)s, CASE WHEN %(status)s = 'ok' THEN %(now)s END, %(details)s, %(now)s)
ON CONFLICT (component) DO UPDATE SET
    status = EXCLUDED.status,
    last_heartbeat = CASE WHEN EXCLUDED.status = 'ok' THEN EXCLUDED.checked_at ELSE pipeline_health.last_heartbeat END,
    details = EXCLUDED.details,
    checked_at = EXCLUDED.checked_at
"""


def sync_alert(cur, r: Result, now: datetime) -> str | None:
    """One open alert per component: opened on degraded/down, resolved on ok. Returns what it did."""
    cur.execute(
        "SELECT id, severity FROM alerts WHERE alert_type = 'pipeline_health' AND entity_id = %s AND resolved = false ORDER BY id DESC",
        (r.component,),
    )
    open_rows = cur.fetchall()
    severity = "critical" if r.status == "down" else "warning"
    message = f"{r.component}: {r.message}"

    if r.status == "ok":
        if open_rows:
            cur.execute("UPDATE alerts SET resolved = true WHERE id = ANY(%s)", ([row[0] for row in open_rows],))
            return "resolved"
        return None
    if r.status not in ("degraded", "down") or not r.alert:
        return None  # unknown / upstream-blocked: leave any open alert as it is
    if not open_rows:
        cur.execute(
            "INSERT INTO alerts (alert_type, severity, entity_id, message, triggered_at, resolved) VALUES ('pipeline_health', %s, %s, %s, %s, false)",
            (severity, r.component, message, now),
        )
        return "opened"
    keep_id, keep_sev = open_rows[0]
    if len(open_rows) > 1:
        cur.execute("UPDATE alerts SET resolved = true WHERE id = ANY(%s)", ([row[0] for row in open_rows[1:]],))
    if keep_sev != severity:
        cur.execute("UPDATE alerts SET severity = %s, message = %s WHERE id = %s", (severity, message, keep_id))
        return "escalated" if severity == "critical" else "downgraded"
    return None


def write_results(conn, results: list[Result], now: datetime) -> dict[str, str | None]:
    actions = {}
    with conn.cursor() as cur:
        for r in results:
            cur.execute(UPSERT_SQL, {"component": r.component, "status": r.status, "now": now,
                                     "details": json.dumps({"message": r.message, **r.details}, default=str)})
            actions[r.component] = sync_alert(cur, r, now)
    conn.commit()
    return actions


def read_newest_timestamps(conn) -> tuple[datetime | None, datetime | None]:
    with conn.cursor() as cur:
        cur.execute("SELECT (SELECT max(window_end) FROM realtime_fleet_metrics), (SELECT max(report_date) FROM vehicle_profitability)")
        speed, batch = cur.fetchone()
    return speed, batch


# ------------------------------------------------------------------ main loop
class State:
    def __init__(self, now: datetime):
        self.tracker = OffsetTracker(now)
        self.airflow = AirflowClient()
        self.airflow_strikes = 0
        self.last_airflow: Result | None = None
        self.producer_status: str | None = None
        self.producer_ok_since: datetime | None = None
        self.last_status: dict[str, str] = {}


def run_cycle(state: State, now: datetime) -> list[Result]:
    results: list[Result] = []

    # kafka + producer
    try:
        end_offset, partitions = read_end_offset()
        results.append(Result("kafka", "ok", "broker reachable", {"topic": KAFKA_TOPIC, "partitions": partitions}))
        producer = evaluate_producer(state.tracker.observe(end_offset, now), end_offset)
    except Exception as exc:  # noqa: BLE001 - any failure here means "cannot see Kafka"
        results.append(Result("kafka", "down", f"cannot read Kafka: {type(exc).__name__}: {exc}"[:300], {"topic": KAFKA_TOPIC}))
        producer = Result("producer", "unknown", "Kafka unreachable", {}, alert=False)
    results.append(producer)

    # producer-gating bookkeeping for the lag checks
    if producer.status == "ok":
        if state.producer_status not in (None, "ok"):
            state.producer_ok_since = now  # just recovered: start the grace period
        elif state.producer_ok_since is None:
            state.producer_ok_since = LONG_AGO  # healthy since before we started watching
    else:
        state.producer_ok_since = None
    state.producer_status = producer.status

    # speed + batch lag from the database
    conn = connect()
    try:
        speed_newest, batch_newest = read_newest_timestamps(conn)
        results.append(evaluate_lag("speed_layer", speed_newest, now, SPEED_STALE_S, SPEED_DOWN_S, producer.status, state.producer_ok_since))
        results.append(evaluate_lag("batch_layer", batch_newest, now, BATCH_STALE_S, BATCH_DOWN_S, producer.status, state.producer_ok_since))

        # airflow
        try:
            airflow = evaluate_airflow(state.airflow.dag_runs(), now)
            state.airflow_strikes = 0
        except Exception as exc:  # noqa: BLE001
            state.airflow_strikes += 1
            if state.airflow_strikes >= AIRFLOW_UNREACHABLE_STRIKES:
                airflow = Result("airflow_dag", "down", f"Airflow API unreachable: {type(exc).__name__}: {exc}"[:300], {})
            else:  # debounce: Airflow takes ~1 min to start, do not flap
                airflow = state.last_airflow or Result("airflow_dag", "unknown", "Airflow API not reachable yet", {}, alert=False)
        state.last_airflow = airflow
        results.append(airflow)

        actions = write_results(conn, results, now)
    finally:
        conn.close()

    for r in results:
        old = state.last_status.get(r.component)
        if old != r.status:
            level = log.warning if r.status in ("degraded", "down") else log.info
            level("health_status_changed", extra={"health_component": r.component, "old": old, "new": r.status,
                                                  "detail": r.message, "alert_action": actions.get(r.component)})
        state.last_status[r.component] = r.status
    return results


def main() -> None:
    log.info("healthcheck_starting", extra={"interval_seconds": CHECK_INTERVAL_S, "kafka": KAFKA_BOOTSTRAP,
                                            "topic": KAFKA_TOPIC, "airflow": AIRFLOW_URL})
    state = State(utcnow())
    while True:
        started = time.monotonic()
        try:
            run_cycle(state, utcnow())
        except Exception:  # noqa: BLE001 - keep the checker alive; a failing cycle is itself logged as an error
            log.exception("health_cycle_failed")
        time.sleep(max(1.0, CHECK_INTERVAL_S - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
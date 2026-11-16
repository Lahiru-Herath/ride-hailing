"""
Unit tests for observability/healthcheck.py.

The evaluators are pure functions, so the decision logic (when is the producer "down", when is lag "stale",
when is an Airflow run a problem) is tested without Kafka, Postgres or Airflow. The Airflow client is tested
against a throwaway local HTTP server that imitates the real /auth/token and dagRuns endpoints.

Run:  uv run pytest -q
"""

import json
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from observability import healthcheck as h

T0 = datetime(2026, 11, 16, 12, 0, 0)


def at(seconds: float) -> datetime:
    return T0 + timedelta(seconds=seconds)


# ------------------------------------------------------------------ producer (Kafka end offset)
def test_offset_tracker_measures_how_long_the_offset_has_not_changed():
    tracker = h.OffsetTracker(T0)
    assert tracker.observe(100, at(0)) == 0
    assert tracker.observe(150, at(15)) == 0  # advanced -> timer restarts
    assert tracker.observe(150, at(45)) == 30  # unchanged for 30 s


def test_offset_tracker_treats_a_topic_reset_as_a_change():
    tracker = h.OffsetTracker(T0)
    tracker.observe(5000, at(0))
    assert tracker.observe(10, at(120)) == 0  # offset went DOWN (topic recreated) - not a stall


@pytest.mark.parametrize(
    "stalled, expected",
    [(0, "ok"), (29, "ok"), (30, "degraded"), (59, "degraded"), (60, "down"), (600, "down")],
)
def test_producer_thresholds(stalled, expected):
    assert h.evaluate_producer(stalled, end_offset=1).status == expected


# ------------------------------------------------------------------ lag of the speed / batch layer
def lag(seconds, producer="ok", ok_since=None, stale=600, down=1200):
    return h.evaluate_lag("speed_layer", at(0) - timedelta(seconds=seconds), at(0), stale, down, producer, ok_since)


def test_normal_speed_layer_lag_is_not_flagged():
    # D44: 3-8 minutes is the measured normal range
    assert lag(180).status == "ok"
    assert lag(460).status == "ok"
    assert lag(599).status == "ok"


def test_stale_and_down_lag():
    assert lag(601).status == "degraded"
    assert lag(1201).status == "down"


def test_lag_is_not_blamed_on_the_layer_while_the_producer_is_down():
    result = lag(900, producer="down")
    assert result.status == "degraded"
    assert result.alert is False  # no alert of its own: the root cause is the producer
    assert result.details["blocked_by"] == "producer"


def test_recovery_grace_period_suppresses_alerts_then_expires():
    recovering = lag(900, ok_since=at(-120))
    assert recovering.status == "degraded" and recovering.alert is False
    after_grace = lag(900, ok_since=at(-700))
    assert after_grace.status == "degraded" and after_grace.alert is True


def test_no_data_yet_is_unknown_not_an_alert():
    result = h.evaluate_lag("batch_layer", None, at(0), 720, 1200, "ok", None)
    assert result.status == "unknown" and result.alert is False


# ------------------------------------------------------------------ Airflow DAG runs
def run(run_id, state, run_after, start=None, end=None):
    return {"dag_run_id": run_id, "state": state, "run_after": run_after, "start_date": start, "end_date": end}


NOW = datetime(2026, 11, 16, 13, 0, 0)
GOOD = [
    run("a", "success", "2026-11-16T12:40:00Z", "2026-11-16T12:41:00Z", "2026-11-16T12:44:00Z"),
    run("b", "success", "2026-11-16T12:45:00Z", "2026-11-16T12:46:00.5Z", "2026-11-16T12:49:00Z"),
]


def test_airflow_ok_when_recent_runs_succeed():
    assert h.evaluate_airflow(GOOD, NOW).status == "ok"


def test_airflow_unknown_without_runs():
    assert h.evaluate_airflow([], NOW).status == "unknown"


def test_airflow_down_when_latest_finished_run_failed():
    runs = GOOD + [run("c", "failed", "2026-11-16T12:50:00Z", "2026-11-16T12:51:00Z", "2026-11-16T12:53:00Z")]
    result = h.evaluate_airflow(runs, NOW)
    assert result.status == "down" and result.details["failed_run"] == "c"


def test_airflow_degraded_after_recovery_until_the_failure_window_passes():
    runs = GOOD + [
        run("c", "failed", "2026-11-16T12:50:00Z", "2026-11-16T12:51:00Z", "2026-11-16T12:53:00Z"),
        run("d", "success", "2026-11-16T12:55:00Z", "2026-11-16T12:55:30Z", "2026-11-16T12:57:00Z"),
    ]
    assert h.evaluate_airflow(runs, NOW).status == "degraded"
    assert h.evaluate_airflow(runs, NOW + timedelta(minutes=40)).status == "ok"


def test_airflow_flags_a_run_that_has_been_running_too_long():
    young = GOOD + [run("e", "running", "2026-11-16T12:50:00Z", "2026-11-16T12:51:00Z", None)]  # 9 min
    old = GOOD + [run("e", "running", "2026-11-16T12:40:00Z", "2026-11-16T12:41:00Z", None)]  # 19 min
    assert h.evaluate_airflow(young, NOW).status == "ok"
    assert h.evaluate_airflow(old, NOW).status == "degraded"


# ------------------------------------------------------------------ Airflow REST client (JWT login, 401 -> re-login)
@pytest.fixture
def fake_airflow(monkeypatch):
    state = {"logins": 0, "expire_next": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path != "/auth/token" or body != {"username": "admin", "password": "admin"}:
                self.send_response(401)
                self.end_headers()
                return
            state["logins"] += 1
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"access_token": f"tok{state['logins']}"}).encode())

        def do_GET(self):
            if self.headers.get("Authorization") != f"Bearer tok{state['logins']}" or state["expire_next"]:
                state["expire_next"] = False
                self.send_response(401)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"dag_runs": GOOD, "total_entries": len(GOOD)}).encode())

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(h, "AIRFLOW_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(h, "AIRFLOW_USER", "admin")
    monkeypatch.setattr(h, "AIRFLOW_PASSWORD", "admin")
    yield state
    server.shutdown()


def test_airflow_client_logs_in_once_and_reuses_the_token(fake_airflow):
    client = h.AirflowClient()
    assert len(client.dag_runs()) == 2
    assert len(client.dag_runs()) == 2
    assert fake_airflow["logins"] == 1


def test_airflow_client_relogs_in_after_a_401(fake_airflow):
    client = h.AirflowClient()
    client.dag_runs()
    fake_airflow["expire_next"] = True  # simulate an expired token
    assert len(client.dag_runs()) == 2
    assert fake_airflow["logins"] == 2
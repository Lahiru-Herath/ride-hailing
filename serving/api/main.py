"""
serving/api/main.py

Serving layer (Phase 5): read-only FastAPI service over the two Lambda views.
  - speed layer  -> realtime_fleet_metrics, alerts   (Spark, ~3 min behind the clock)
  - batch layer  -> vehicle_profitability            (Airflow, one block per simulated day)

All DB timestamps are naive UTC; every datetime leaves this API tagged UTC ("...Z").
"""

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from contextvars import ContextVar
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import AfterValidator, BaseModel
from sqlalchemy import create_engine, func, or_, select, text
from sqlalchemy.orm import Session, sessionmaker

from observability.logging_config import get_logger
from storage.models import Alert, PipelineHealth, RealtimeFleetMetric, VehicleProfitability

try:  # host runs read DATABASE_URL from .env; the container gets it from compose
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set (see .env / docker-compose.yml)")

WINDOW = timedelta(minutes=5)
DASHBOARD_PATH = Path(__file__).with_name("dashboard.html")

log = get_logger("api")

# --- request correlation (D55): every request gets an id, returned in the X-Request-ID header and attached
# to every log line written while the request is handled. A caller-supplied X-Request-ID is reused, so one id
# can be followed from the client through the API logs.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


log.addFilter(RequestIdFilter())

# --- in-process request metrics (D56), exposed by /metrics in Prometheus text format
_metrics_lock = threading.Lock()
_req_count: dict[tuple[str, str, int], int] = defaultdict(int)  # (method, route, status) -> requests
_req_dur_sum: dict[str, float] = defaultdict(float)  # route -> seconds
_req_dur_cnt: dict[str, int] = defaultdict(int)
_started_at = time.time()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine)

M = RealtimeFleetMetric
V = VehicleProfitability


# --------------------------------------------------------------------------- helpers
def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


UtcDatetime = Annotated[datetime, AfterValidator(_as_utc)]


def _now_naive() -> datetime:
    """Current UTC time as a naive datetime, comparable with the DB columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _lag_seconds(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    return round((_now_naive() - ts).total_seconds(), 1)


def get_db() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


DbSession = Annotated[Session, Depends(get_db)]


# --------------------------------------------------------------------------- schemas
class ZoneMetrics(BaseModel):
    zone: str
    active_vehicles: int  # vehicles seen non-idle in this zone/window (NOT additive across zones)
    idle_vehicles: int  # vehicles seen idle in this zone/window (NOT additive across zones)
    trips_completed: int
    total_earnings: float


class WindowMetrics(BaseModel):
    window_start: UtcDatetime
    window_end: UtcDatetime
    trips_completed: int
    total_earnings: float
    zones: list[ZoneMetrics]


class RealtimeResponse(BaseModel):
    data_through: UtcDatetime | None
    lag_seconds: float | None
    windows: list[WindowMetrics]  # newest first


class DaySummary(BaseModel):
    report_date: UtcDatetime  # end of the simulated day
    vehicles: int
    trips_completed: int
    total_earnings: float
    fuel_cost: float
    maintenance_cost: float
    net_profit: float
    distance_covered_km: float
    profitable_vehicles: int


class VehicleDay(BaseModel):
    report_date: UtcDatetime
    vehicle_id: str
    trips_completed: int
    total_earnings: float
    fuel_cost: float
    maintenance_cost: float
    distance_covered_km: float
    net_profit: float
    is_profitable: bool
    cost_per_km: float | None
    service_flag: bool


class ProfitabilityResponse(BaseModel):
    latest: DaySummary | None
    vehicles: list[VehicleDay]  # latest simulated day, best net profit first
    history: list[DaySummary]  # newest first, includes the latest day


class AlertOut(BaseModel):
    id: int
    alert_type: str
    severity: str
    entity_id: str | None
    message: str | None
    triggered_at: UtcDatetime
    resolved: bool
    age_minutes: float
    active: bool


class AlertsResponse(BaseModel):
    active_minutes: int
    active_count: int
    total_count: int
    alerts: list[AlertOut]  # newest first


class SinceBatch(BaseModel):
    windows: int
    trips_completed: int
    total_earnings: float
    first_window_start: UtcDatetime | None
    last_window_end: UtcDatetime | None


class Freshness(BaseModel):
    speed_data_through: UtcDatetime | None
    speed_lag_seconds: float | None
    batch_through: UtcDatetime | None
    batch_lag_seconds: float | None


class OverviewResponse(BaseModel):
    now: UtcDatetime
    batch: DaySummary | None
    speed_latest_window: WindowMetrics | None
    speed_since_batch: SinceBatch
    freshness: Freshness


# --------------------------------------------------------------------------- queries
def _load_windows(db: Session, n: int) -> list[WindowMetrics]:
    """Newest n 5-minute windows, de-duplicated on (window_start, zone) - latest insert wins (D38)."""
    newest = db.scalar(select(func.max(M.window_start)))
    if newest is None:
        return []
    since = newest - (n - 1) * WINDOW
    rows = db.scalars(
        select(M)
        .where(M.window_start >= since)
        .distinct(M.window_start, M.zone)
        .order_by(M.window_start.desc(), M.zone, M.id.desc())
    ).all()

    grouped: dict[datetime, list[RealtimeFleetMetric]] = {}
    for row in rows:
        grouped.setdefault(row.window_start, []).append(row)

    windows = []
    for start, items in grouped.items():
        zones = [
            ZoneMetrics(
                zone=i.zone,
                active_vehicles=i.active_vehicles or 0,
                idle_vehicles=i.idle_vehicles or 0,
                trips_completed=i.trips_completed or 0,
                total_earnings=float(i.total_earnings or 0),
            )
            for i in items
        ]
        windows.append(
            WindowMetrics(
                window_start=start,
                window_end=items[0].window_end,
                trips_completed=sum(z.trips_completed for z in zones),
                total_earnings=round(sum(z.total_earnings for z in zones), 2),
                zones=zones,
            )
        )
    return windows


def _day_summaries(db: Session, days: int) -> list[DaySummary]:
    dates = db.scalars(
        select(V.report_date).distinct().order_by(V.report_date.desc()).limit(days)
    ).all()
    if not dates:
        return []
    rows = db.execute(
        select(
            V.report_date,
            func.count().label("vehicles"),
            func.coalesce(func.sum(V.trips_completed), 0).label("trips"),
            func.coalesce(func.sum(V.total_earnings), 0).label("earnings"),
            func.coalesce(func.sum(V.fuel_cost), 0).label("fuel"),
            func.coalesce(func.sum(V.maintenance_cost), 0).label("maint"),
            func.coalesce(func.sum(V.net_profit), 0).label("net"),
            func.coalesce(func.sum(V.distance_covered_km), 0).label("dist"),
            func.count().filter(V.is_profitable).label("profitable"),
        )
        .where(V.report_date >= dates[-1])
        .group_by(V.report_date)
        .order_by(V.report_date.desc())
    ).all()
    return [
        DaySummary(
            report_date=r.report_date,
            vehicles=r.vehicles,
            trips_completed=int(r.trips),
            total_earnings=float(r.earnings),
            fuel_cost=float(r.fuel),
            maintenance_cost=float(r.maint),
            net_profit=float(r.net),
            distance_covered_km=float(r.dist),
            profitable_vehicles=r.profitable,
        )
        for r in rows
    ]


def _vehicle_day(r: VehicleProfitability) -> VehicleDay:
    return VehicleDay(
        report_date=r.report_date,
        vehicle_id=r.vehicle_id,
        trips_completed=r.trips_completed or 0,
        total_earnings=float(r.total_earnings or 0),
        fuel_cost=float(r.fuel_cost or 0),
        maintenance_cost=float(r.maintenance_cost or 0),
        distance_covered_km=float(r.distance_covered_km or 0),
        net_profit=float(r.net_profit or 0),
        is_profitable=bool(r.is_profitable),
        cost_per_km=float(r.cost_per_km) if r.cost_per_km is not None else None,
        service_flag=bool(r.service_flag),
    )


# --------------------------------------------------------------------------- app
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("api_started")
    yield
    engine.dispose()
    log.info("api_stopped")


app = FastAPI(
    title="Ride-Hailing Fleet Operations - Serving Layer",
    description="Lambda serving layer: speed-layer metrics + batch-layer profitability. "
    "All timestamps are UTC; money is LKR.",
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    token = request_id_var.set(rid)
    start = time.perf_counter()
    try:
        try:
            response = await call_next(request)
        except Exception:
            log.exception("request_failed", extra={"http_method": request.method, "http_path": request.url.path})
            _record_request(request, 500, time.perf_counter() - start)
            # The traceback is in the log under this request_id; the caller gets the id to quote when reporting it.
            return JSONResponse(
                {"detail": "internal server error", "request_id": rid}, status_code=500, headers={"X-Request-ID": rid}
            )
        duration = time.perf_counter() - start
        _record_request(request, response.status_code, duration)
        response.headers["X-Request-ID"] = rid
        log.info(
            "request",
            extra={
                "http_method": request.method,
                "http_path": request.url.path,
                "http_status": response.status_code,
                "duration_ms": round(duration * 1000, 1),
            },
        )
        return response
    finally:
        request_id_var.reset(token)


def _record_request(request: Request, status: int, seconds: float) -> None:
    route = request.scope.get("route")
    path = getattr(route, "path", None) or "unmatched"  # route template, so /fleet/profitability/{vehicle_id} is one series
    with _metrics_lock:
        _req_count[(request.method, path, status)] += 1
        _req_dur_sum[path] += seconds
        _req_dur_cnt[path] += 1


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(DASHBOARD_PATH, media_type="text/html")


HEALTH_STALE_SECONDS = 60  # the healthcheck service writes every ~15 s; older than this = it stopped reporting
_SEVERITY = {"ok": 0, "unknown": 1, "degraded": 2, "down": 3}


def _pipeline_health(db: Session) -> dict:
    """Pipeline block for /health, read from pipeline_health (written by the healthcheck service, D46)."""
    rows = db.scalars(select(PipelineHealth).order_by(PipelineHealth.component)).all()
    if not rows:
        return {"status": "unknown", "message": "healthcheck has not reported yet", "components": []}
    now = _now_naive()
    components = []
    for r in rows:
        try:
            detail = json.loads(r.details) if r.details else {}
        except ValueError:
            detail = {}
        components.append(
            {
                "component": r.component,
                "status": r.status,
                "message": detail.get("message"),
                "checked_at": _as_utc(r.checked_at),
                "last_ok": _as_utc(r.last_heartbeat) if r.last_heartbeat else None,
            }
        )
    age = (now - max(r.checked_at for r in rows)).total_seconds()
    if age > HEALTH_STALE_SECONDS:
        return {"status": "unknown", "message": f"healthcheck not reporting for {round(age)} s", "components": components}
    worst = max(components, key=lambda c: _SEVERITY.get(c["status"], 1))["status"]
    message = "all components ok" if worst == "ok" else f"worst component status: {worst}"
    return {"status": worst, "message": message, "components": components}


@app.get("/health")
def health(db: DbSession):
    """API + database reachability, plus pipeline health (Kafka, producer, speed/batch lag, Airflow DAG).

    HTTP 503 only when the database is unreachable. A degraded or down pipeline is reported in the body
    with HTTP 200, so the API container's own Docker healthcheck is not tripped by an upstream problem (D51).
    """
    try:
        db.execute(text("SELECT 1"))
    except Exception:
        log.exception("health_db_failed")
        raise HTTPException(status_code=503, detail="database unavailable")
    try:
        pipeline = _pipeline_health(db)
    except Exception:
        log.exception("health_pipeline_failed")
        pipeline = {"status": "unknown", "message": "pipeline_health unreadable", "components": []}
    overall = "ok" if pipeline["status"] == "ok" else pipeline["status"]
    return {"status": overall, "database": "ok", "pipeline": pipeline, "checked_at": datetime.now(timezone.utc)}


_STATUS_CODE = {"ok": 0, "degraded": 1, "down": 2, "unknown": 3}


def _prom_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


@app.get("/metrics", include_in_schema=False)
def metrics(db: DbSession):
    """Prometheus text exposition (v0.0.4): API request metrics plus the pipeline's health and freshness.

    Pipeline values are read from the same tables the dashboard uses, so a scraper sees what an operator sees.
    """
    lines: list[str] = []

    def metric(name: str, kind: str, help_text: str, samples: list[tuple[dict[str, str], float]]) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        for labels, value in samples:
            label_text = ",".join(f'{k}="{_prom_escape(v)}"' for k, v in labels.items())
            lines.append(f"{name}{{{label_text}}} {value}" if label_text else f"{name} {value}")

    with _metrics_lock:
        counts = dict(_req_count)
        dur_sum, dur_cnt = dict(_req_dur_sum), dict(_req_dur_cnt)
    metric("api_up", "gauge", "1 while the API process is serving.", [({}, 1)])
    metric("api_uptime_seconds", "gauge", "Seconds since the API process started.", [({}, round(time.time() - _started_at, 1))])
    metric("api_http_requests_total", "counter", "HTTP requests by method, route template and status.",
           [({"method": m, "path": p, "status": str(c)}, n) for (m, p, c), n in sorted(counts.items())])
    metric("api_http_request_duration_seconds_sum", "counter", "Total handling time by route template.",
           [({"path": p}, round(v, 6)) for p, v in sorted(dur_sum.items())])
    metric("api_http_request_duration_seconds_count", "counter", "Requests timed, by route template.",
           [({"path": p}, n) for p, n in sorted(dur_cnt.items())])

    pipeline = _pipeline_health(db)
    comps = pipeline["components"]
    metric("pipeline_component_status", "gauge", "Component status from the healthcheck: 0 ok, 1 degraded, 2 down, 3 unknown.",
           [({"component": c["component"]}, _STATUS_CODE.get(c["status"], 3)) for c in comps])
    now = datetime.now(timezone.utc)
    metric("pipeline_component_check_age_seconds", "gauge", "Seconds since the healthcheck last evaluated the component.",
           [({"component": c["component"]}, round((now - c["checked_at"]).total_seconds(), 1)) for c in comps])
    metric("pipeline_status", "gauge", "Overall pipeline status (same codes); 3 if the healthcheck stopped reporting.",
           [({}, _STATUS_CODE.get(pipeline["status"], 3))])

    speed_through = db.scalar(select(func.max(M.window_end)))
    batch_through = db.scalar(select(func.max(V.report_date)))
    metric("speed_layer_lag_seconds", "gauge", "Age of the newest speed-layer window end.",
           [({}, _lag_seconds(speed_through))] if speed_through else [])
    metric("batch_layer_lag_seconds", "gauge", "Age of the newest batch result (simulated day end).",
           [({}, _lag_seconds(batch_through))] if batch_through else [])

    open_pipeline = db.scalar(select(func.count()).select_from(Alert).where(Alert.alert_type == "pipeline_health", Alert.resolved.is_(False)))
    recent_idle = db.scalar(
        select(func.count()).select_from(Alert).where(
            Alert.alert_type != "pipeline_health", Alert.resolved.is_(False), Alert.triggered_at >= _now_naive() - timedelta(minutes=15)
        )
    )
    metric("alerts_open", "gauge", "Unresolved pipeline_health alerts, and idle-vehicle alerts fired in the last 15 minutes.",
           [({"kind": "pipeline_health"}, open_pipeline or 0), ({"kind": "idle_vehicle"}, recent_idle or 0)])

    return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/fleet/realtime", response_model=RealtimeResponse)
def fleet_realtime(db: DbSession, windows: Annotated[int, Query(ge=1, le=72)] = 6):
    """Speed layer: newest 5-minute windows by zone. Vehicle counts are per zone and not additive."""
    data = _load_windows(db, windows)
    through = data[0].window_end if data else None
    # lag is measured against the naive DB value, so convert back for the comparison
    lag = _lag_seconds(through.replace(tzinfo=None)) if through else None
    return RealtimeResponse(data_through=through, lag_seconds=lag, windows=data)


@app.get("/fleet/profitability", response_model=ProfitabilityResponse)
def fleet_profitability(db: DbSession, days: Annotated[int, Query(ge=1, le=288)] = 12):
    """Batch layer: latest simulated day per vehicle, plus per-day totals for the last `days` days."""
    history = _day_summaries(db, days)
    if not history:
        return ProfitabilityResponse(latest=None, vehicles=[], history=[])
    latest_date = db.scalar(select(func.max(V.report_date)))
    rows = db.scalars(
        select(V).where(V.report_date == latest_date).order_by(V.net_profit.desc(), V.vehicle_id)
    ).all()
    return ProfitabilityResponse(
        latest=history[0], vehicles=[_vehicle_day(r) for r in rows], history=history
    )


@app.get("/fleet/profitability/{vehicle_id}", response_model=list[VehicleDay])
def vehicle_profitability(
    vehicle_id: str, db: DbSession, days: Annotated[int, Query(ge=1, le=288)] = 12
):
    """Batch layer: one vehicle's results over its last `days` simulated days, newest first."""
    rows = db.scalars(
        select(V).where(V.vehicle_id == vehicle_id).order_by(V.report_date.desc()).limit(days)
    ).all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"no profitability rows for vehicle {vehicle_id}")
    return [_vehicle_day(r) for r in rows]


@app.get("/alerts", response_model=AlertsResponse)
def alerts(
    db: DbSession,
    active_minutes: Annotated[int, Query(ge=1, le=1440)] = 15,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
):
    """Newest alerts. Idle-vehicle alerts are 'active' while unresolved and triggered within `active_minutes` (D40);
    pipeline_health alerts stay active until the healthcheck resolves them (D52)."""
    now = _now_naive()
    cutoff = now - timedelta(minutes=active_minutes)
    active_count = db.scalar(
        select(func.count())
        .select_from(Alert)
        .where(Alert.resolved.is_(False), or_(Alert.alert_type == "pipeline_health", Alert.triggered_at >= cutoff))
    )
    total_count = db.scalar(select(func.count()).select_from(Alert))
    rows = db.scalars(select(Alert).order_by(Alert.triggered_at.desc(), Alert.id.desc()).limit(limit)).all()
    items = [
        AlertOut(
            id=a.id,
            alert_type=a.alert_type,
            severity=a.severity,
            entity_id=a.entity_id,
            message=a.message,
            triggered_at=a.triggered_at,
            resolved=a.resolved,
            age_minutes=round((now - a.triggered_at).total_seconds() / 60, 1),
            active=(not a.resolved) and (a.alert_type == "pipeline_health" or a.triggered_at >= cutoff),
        )
        for a in rows
    ]
    return AlertsResponse(
        active_minutes=active_minutes, active_count=active_count or 0, total_count=total_count or 0, alerts=items
    )


@app.get("/fleet/overview", response_model=OverviewResponse)
def fleet_overview(db: DbSession):
    """
    Merged Lambda view: the authoritative batch result for the latest completed simulated day, plus
    the speed layer's windows that end after the batch cutoff (not yet covered by batch).
    Approximate by design: batch days end at arbitrary instants, speed windows on 5-minute boundaries (D41).
    """
    batch_list = _day_summaries(db, 1)
    batch = batch_list[0] if batch_list else None
    latest_windows = _load_windows(db, 1)
    latest_window = latest_windows[0] if latest_windows else None

    since = SinceBatch(windows=0, trips_completed=0, total_earnings=0.0, first_window_start=None, last_window_end=None)
    batch_through_naive = db.scalar(select(func.max(V.report_date)))
    if batch_through_naive is not None:
        dedup = (
            select(M)
            .where(M.window_end > batch_through_naive)
            .distinct(M.window_start, M.zone)
            .order_by(M.window_start, M.zone, M.id.desc())
            .subquery()
        )
        agg = db.execute(
            select(
                func.count(func.distinct(dedup.c.window_start)),
                func.coalesce(func.sum(dedup.c.trips_completed), 0),
                func.coalesce(func.sum(dedup.c.total_earnings), 0),
                func.min(dedup.c.window_start),
                func.max(dedup.c.window_end),
            )
        ).one()
        since = SinceBatch(
            windows=agg[0],
            trips_completed=int(agg[1]),
            total_earnings=float(agg[2]),
            first_window_start=agg[3],
            last_window_end=agg[4],
        )

    speed_through = latest_window.window_end if latest_window else None
    return OverviewResponse(
        now=datetime.now(timezone.utc),
        batch=batch,
        speed_latest_window=latest_window,
        speed_since_batch=since,
        freshness=Freshness(
            speed_data_through=speed_through,
            speed_lag_seconds=_lag_seconds(speed_through.replace(tzinfo=None)) if speed_through else None,
            batch_through=batch.report_date if batch else None,
            batch_lag_seconds=_lag_seconds(batch_through_naive),
        ),
    )
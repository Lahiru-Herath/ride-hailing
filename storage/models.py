from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RealtimeFleetMetric(Base):
    __tablename__ = "realtime_fleet_metrics"

    id: Mapped[int] = mapped_column(primary_key=True)
    window_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    zone: Mapped[str] = mapped_column(String(50), nullable=False)
    active_vehicles: Mapped[int | None]
    idle_vehicles: Mapped[int | None]
    trips_completed: Mapped[int | None]
    total_earnings: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class VehicleProfitability(Base):
    __tablename__ = "vehicle_profitability"
    __table_args__ = (
        UniqueConstraint(
            "report_date",
            "vehicle_id",
            name="uq_vehicle_profitability_report_date_vehicle_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    # End of the simulated day this row covers (UTC, naive) - NOT a calendar date (see D27).
    report_date: Mapped[datetime] = mapped_column(nullable=False)
    vehicle_id: Mapped[str] = mapped_column(String(20), nullable=False)
    total_earnings: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    fuel_cost: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    maintenance_cost: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    distance_covered_km: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    net_profit: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    is_profitable: Mapped[bool | None] = mapped_column(Boolean)
    trips_completed: Mapped[int | None]
    cost_per_km: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    service_flag: Mapped[bool | None] = mapped_column(Boolean)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(primary_key=True)
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    entity_id: Mapped[str | None] = mapped_column(String(50))
    message: Mapped[str | None] = mapped_column(Text)
    triggered_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)


class PipelineHealth(Base):
    __tablename__ = "pipeline_health"

    id: Mapped[int] = mapped_column(primary_key=True)
    component: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    last_heartbeat: Mapped[datetime | None] = mapped_column(DateTime)
    details: Mapped[str | None] = mapped_column(Text)
    checked_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
"""
sources/batch_generator.py

Once per simulated day (SIM_DAY_DURATION_SECONDS), writes a CSV of
per-vehicle cost data to the batch-incoming folder watched by the Phase 4
Airflow DAG.

Filename: vehicle_costs_<UTC window end as YYYYMMDDTHHMMSSZ>.csv  (see D24).
The CSV covers the simulated day that ended at that instant, i.e. the window
[window_end - SIM_DAY_DURATION_SECONDS, window_end). It is written under a
hidden temporary name and renamed into place, so a watcher never sees a
half-written CSV.

Schema: vehicle_id, fuel_cost, maintenance_cost, distance_covered_km, service_flag.

Cost scale (D33): one simulated day is only SIM_DAY_DURATION_SECONDS of real time, in which a
vehicle completes about 1-2 trips. Distance and costs are therefore scaled to that compressed
day, so that earnings and costs are the same order of magnitude and vehicles show a realistic
mix of profit and loss. Costs stay independent of the producer: they model an external,
authoritative source (fuel cards / workshop invoices).

Run with: uv run python sources/batch_generator.py
"""

import csv
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent.parent))
from observability.logging_config import get_logger

load_dotenv()

logger = get_logger("batch_generator")

SIM_DAY_DURATION_SECONDS = float(os.environ["SIM_DAY_DURATION_SECONDS"])
NUM_VEHICLES = int(os.environ["NUM_VEHICLES"])
BATCH_OUTPUT_DIR = Path(os.environ.get("BATCH_OUTPUT_DIR", "storage/batch_incoming"))

FIELDNAMES = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered_km", "service_flag"]


def generate_day_rows() -> list[dict]:
    rows = []
    for i in range(1, NUM_VEHICLES + 1):
        vehicle_id = f"V{i:03d}"
        distance_km = round(random.uniform(10, 45), 1)  # compressed simulated day (D33)
        fuel_cost = round(distance_km * random.uniform(25.0, 40.0), 2)  # LKR/km
        service_flag = random.random() < 0.08  # ~8% of vehicles get serviced that day
        maintenance_cost = round(
            random.uniform(1500, 4000) if service_flag else random.uniform(0, 400), 2
        )
        rows.append(
            {
                "vehicle_id": vehicle_id,
                "fuel_cost": fuel_cost,
                "maintenance_cost": maintenance_cost,
                "distance_covered_km": distance_km,
                "service_flag": service_flag,
            }
        )
    return rows


def write_batch_file(sim_day_index: int) -> Path:
    BATCH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    window_end = datetime.now(timezone.utc)
    stamp = window_end.strftime("%Y%m%dT%H%M%SZ")
    filepath = BATCH_OUTPUT_DIR / f"vehicle_costs_{stamp}.csv"
    tmp_path = BATCH_OUTPUT_DIR / f".vehicle_costs_{stamp}.csv.tmp"

    rows = generate_day_rows()
    with tmp_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, filepath)  # atomic rename: watchers only ever see complete files

    logger.info(
        "batch_file_written",
        extra={
            "file": str(filepath),
            "row_count": len(rows),
            "sim_day_index": sim_day_index,
            "window_end": window_end.isoformat(),
        },
    )
    return filepath


def main() -> None:
    logger.info(
        "batch_generator_starting",
        extra={
            "sim_day_duration_seconds": SIM_DAY_DURATION_SECONDS,
            "output_dir": str(BATCH_OUTPUT_DIR),
        },
    )

    sim_day_index = 0
    try:
        while True:
            time.sleep(SIM_DAY_DURATION_SECONDS)
            sim_day_index += 1
            write_batch_file(sim_day_index)
    except KeyboardInterrupt:
        logger.info("batch_generator_shutdown_requested")


if __name__ == "__main__":
    main()
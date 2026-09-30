"""
Once per simulated day (SIM_DAY_DURATION_SECONDS), writes a CSV of per-vehicle cost data to the batch-incoming folder that the Phase 4 Airflow DAG will watch.

Schema; vehicle_id, fuel_cost, maintenance_cost, distance_covered_km, service_flag.

Run with: uv run python sources/batch_generator.py
"""

import csv
import os
import random
import sys
import time
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
        distance_km = round(random.uniform(60, 320), 1)
        fuel_cost = round(distance_km * random.uniform(25.0, 40.0), 2)  # LKR/km
        service_flag = random.random() < 0.08  # ~8% of vehicles get serviced that day
        maintenance_cost = round(
            random.uniform(5000, 20000) if service_flag else random.uniform(0, 500), 2
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
    filepath = BATCH_OUTPUT_DIR / f"vehicle_costs_day{sim_day_index:03d}.csv"

    rows = generate_day_rows()
    with filepath.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(
        "batch_file_written",
        extra={"file": str(filepath), "row_count": len(rows), "sim_day_index": sim_day_index},
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
"""
Simulates NUM_VEHICLES ride-hailing vehicles moving around a fake city grid, emitting one JSON telemetry event per vehicle to the "trip-events' Kafka topic every STREAM_EVENT_INTERVAL_SECONDS.

Event schema: trip_id, driver_id, vehicle_id, lat, lon, speed, status (idle/enroute/on_trip), fare, timestamp

Run with: uv run python sources/streaming_producer.py
"""

import json
import os
import random
import sys
import time
import uuid

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

sys.path.append(str(Path(__file__).resolve().parent.parent))
from observability.logging_config import get_logger

load_dotenv()

logger = get_logger("streaming_producer")

# Config from .env
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC_TRIP_EVENTS"]
STREAM_EVENT_INTERVAL_SECONDS = float(os.environ["STREAM_EVENT_INTERVAL_SECONDS"])
NUM_VEHICLES = int(os.environ["NUM_VEHICLES"])

# Fake city grid (bounding box standing in for a real map)
LAT_MIN, LAT_MAX = 6.900, 6.960
LON_MIN, LON_MAX = 79.850, 79.900

# Per-tick transition probabilities (tuned for a 3s tick / 5-minute simulated day)
P_IDLE_TO_ENROUTE = 0.15
P_ENROUTE_TO_ON_TRIP = 0.35
P_ON_TRIP_TO_IDLE = 0.12
P_BECOME_STUCK = 0.03                     # chance an idle vehicle gets "stuck" this tick
STUCK_DURATION_TICKS_RANGE = (150, 400)   # ~7.5-20 min of simulated time at 3s/tick


@dataclass
class Vehicle:
    vehicle_id: str
    driver_id: str
    lat: float
    lon: float
    status: str = "idle"
    trip_id: str | None = None
    trip_target_fare: float = 0.0
    trip_fare_so_far: float = 0.0
    stuck_ticks_remaining: int = 0

    @classmethod
    def new(cls, index: int) -> "Vehicle":
        return cls(
            vehicle_id=f"V{index:03d}",
            driver_id=f"D{index:03d}",
            lat=random.uniform(LAT_MIN, LAT_MAX),
            lon=random.uniform(LON_MIN, LON_MAX),
        )

    def move(self) -> None:
        if self.status == "idle":
            return

        step = 0.0015 if self.status == "on_trip" else 0.001
        self.lat = min(LAT_MAX, max(LAT_MIN, self.lat + random.uniform(-step, step)))
        self.lon = min(LON_MAX, max(LON_MIN, self.lon + random.uniform(-step, step)))

    def tick(self) -> None:
        if self.status == "idle":
            if self.stuck_ticks_remaining > 0:
                self.stuck_ticks_remaining -= 1
                return
            if random.random() < P_BECOME_STUCK:
                self.stuck_ticks_remaining = random.randint(*STUCK_DURATION_TICKS_RANGE)
                return
            if random.random() < P_IDLE_TO_ENROUTE:
                self.status = "enroute"

        elif self.status == "enroute":
            if random.random() < P_ENROUTE_TO_ON_TRIP:
                self.status = "on_trip"
                self.trip_id = str(uuid.uuid4())
                self.trip_target_fare = round(random.uniform(300.0, 2500.0), 2) # LKR
                self.trip_fare_so_far = 0.0

        elif self.status == "on_trip":
                increment = self.trip_target_fare * random.uniform(0.1, 0.3)
                self.trip_fare_so_far = min(self.trip_target_fare, self.trip_fare_so_far + increment)
                if random.random() < P_ON_TRIP_TO_IDLE:
                    self.status = "idle"
                    self.trip_fare_so_far = self.trip_target_fare  # finalize fare on the last event of this trip

        self.move()

    def to_event(self) -> dict:
        """
        NOTE for Phase 3 (speed layer): `fare` is the running total for the
        vehicle's CURRENT trip, re-emitted on every on_trip event, not a
        per-event delta. To get a trip's earnings, take the last fare value
        per trip_id in a window (e.g. Spark's last() or max() on fare).
        """
        event = {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": round(random.uniform(0, 60), 1) if self.status != "idle" else 0.0,
            "status": self.status,
            "fare": round(self.trip_fare_so_far, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # trip just completed this tick -> clear state so the next trip starts fresh
        if self.status == "idle" and self.trip_id is not None:
            self.trip_id = None
            self.trip_fare_so_far = 0.0
            self.trip_target_fare = 0.0
        return event

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )

def main() -> None:
    logger.info(
        "producer_starting",
        extra={
            "num_vehicles": NUM_VEHICLES,
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC,
        }
    )

    producer = build_producer()
    vehicles = [Vehicle.new(i + 1) for i in range(NUM_VEHICLES)]

    logger.info("producer_ready", extra={"topic": KAFKA_TOPIC})

    try:
        while True:
            for vehicle in vehicles:
                vehicle.tick()
                event = vehicle.to_event()

                # keyed by vehicle_id so partitioning is sensible (Phase 2 rebric item)
                producer.send(KAFKA_TOPIC, key=vehicle.vehicle_id, value=event)

                logger.info(
                    "event_emitted",
                    extra={
                        "vehicle_id": event["vehicle_id"],
                        "status": event["status"],
                        "trip_id": event["trip_id"],
                    }
                )

            producer.flush()
            time.sleep(STREAM_EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("producer_shutdown_requested")
    finally:
        producer.flush()
        producer.close()

        logger.info("producer_stopped")

if __name__ == "__main__":
    main()
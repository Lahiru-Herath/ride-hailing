"""
Daily reconciliation (Lambda batch layer).

Runs on the simulated-day cadence (every 5 real minutes, D6). Each run handles ALL unprocessed
cost CSVs in storage/batch_incoming/ (D32), one mapped task instance per file:

    wait_for_batch_file -> find_pending_files
        -> wait_for_stream_ready[file] -> reconcile[file] -> check_data_quality[file]
        -> archive_processed_file[file]

wait_for_stream_ready blocks until the speed layer has committed events past the end of that
file's simulated day (D31). A CSV is only moved to storage/batch_processed/ after its quality
check passes, so a failed file stays in place and the next run retries it; the other files in
the same run are unaffected. Re-processing a file is safe because the Postgres write is an
idempotent upsert (D27). If no CSV arrives within the interval the sensor soft-fails and the
run is marked skipped, not failed.
"""

import sys
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import dag, task

# processing/ and observability/ are bind-mounted under /opt/airflow/project (see docker-compose.yml)
sys.path.append("/opt/airflow/project/processing")


@dag(
    dag_id="daily_reconciliation",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["batch", "lambda", "reconciliation"],
    doc_md=__doc__,
)
def daily_reconciliation():
    # Path is relative to the base path in the fs_default connection (/opt/airflow/storage).
    wait_for_batch_file = FileSensor(
        task_id="wait_for_batch_file",
        filepath="batch_incoming/vehicle_costs_*.csv",
        fs_conn_id="fs_default",
        mode="reschedule",
        poke_interval=10,
        timeout=240,
        soft_fail=True,
    )

    @task
    def find_pending_files() -> list[str]:
        from batch_reconciliation import list_pending_files

        return [str(p) for p in list_pending_files()]  # oldest first

    # No task-level retries: on timeout the CSV simply stays in batch_incoming/ and the next
    # scheduled run picks it up again.
    @task(retries=0, execution_timeout=timedelta(minutes=12))
    def wait_for_stream_ready(csv_path: str) -> str:
        from batch_reconciliation import wait_until_stream_ready

        wait_until_stream_ready(csv_path)
        return csv_path

    @task
    def reconcile(csv_path: str) -> dict:
        from batch_reconciliation import reconcile_file

        return {**reconcile_file(csv_path), "csv_path": csv_path}

    @task
    def check_data_quality(summary: dict) -> dict:
        from batch_reconciliation import check_quality

        quality = check_quality(summary["report_date"], summary["csv_rows"])
        return {**quality, "csv_path": summary["csv_path"]}

    @task
    def archive_processed_file(result: dict) -> str:
        from batch_reconciliation import archive_batch_file

        return archive_batch_file(result["csv_path"])

    pending = find_pending_files()
    wait_for_batch_file >> pending

    ready = wait_for_stream_ready.expand(csv_path=pending)
    summaries = reconcile.expand(csv_path=ready)
    quality = check_data_quality.expand(summary=summaries)
    archive_processed_file.expand(result=quality)


daily_reconciliation()
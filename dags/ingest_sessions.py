"""STEP 7d - The real DAG: every day, ingest the uploads that landed in raw that day.

    [list_files] ──► [ingest_day] ──► [summary]

Each DAG run covers ONE day (its "data interval"): the run for 2026-11-16 covers
2026-11-16 00:00 -> 2026-11-17 00:00 UTC and starts right AFTER that day has ended.
All the real work is our own code (neural_lakehouse.pipeline), which is idempotent,
so retries and backfills can re-run any day safely.
"""

from datetime import datetime

from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable


@dag(
    dag_id="ingest_sessions",
    # Every day at 00:00 UTC, process the day that has just finished.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 16),
    catchup=False,
    # One run at a time: our local catalog is a SQLite file, which allows a single writer.
    # (With Glue or a REST catalog, several days could run in parallel.)
    max_active_runs=1,
    tags=["lakehouse"],
)
def ingest_sessions():
    @task
    def list_files(data_interval_start=None) -> dict:
        """Which JSON files landed in raw on the day of this run? (small XCom: just paths)"""
        from neural_lakehouse.pipeline import list_landing
        from neural_lakehouse.utils.s3 import get_s3_filesystem

        day = data_interval_start.date()
        keys = list_landing(get_s3_filesystem(), "raw", day)
        print(f"Day {day}: {len(keys)} uploads found")
        return {"day": day.isoformat(), "json_keys": keys}

    @task
    def ingest_day(files: dict, run_id=None) -> dict:
        """Validate every upload and write to Iceberg (idempotent). run_id goes to the tables."""
        from neural_lakehouse.pipeline import ingest

        result = ingest(files["json_keys"], run_id=run_id)
        print(f"Day {files['day']}: {result}")
        return {"day": files["day"], **result}

    @task
    def summary(result: dict) -> None:
        print(
            f"SUMMARY {result['day']}: accepted={result.get('accepted', 0)} "
            f"quarantined={result.get('quarantined', 0)} skipped={result.get('skipped', 0)}"
        )

    summary(ingest_day(list_files()))


ingest_sessions()

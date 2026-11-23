"""STEP 6 - Ingest the raw zone into Iceberg, ONE DAY PER RUN (like Airflow will do).

Needs: docker compose up -d   and   uv run python -m neural_lakehouse.generator
Run:   uv run python playground/step6_ingest.py           (idempotent: run it as often as you like)
       uv run python playground/step6_ingest.py --reset   (drop the 3 tables first)
"""

import sys
from datetime import date, timedelta

from neural_lakehouse.lakehouse.catalog import get_catalog
from neural_lakehouse.lakehouse.tables import CHANNEL_METRICS, REJECTED_FILES, SESSIONS
from neural_lakehouse.pipeline import ingest, list_landing
from neural_lakehouse.utils.s3 import get_s3_filesystem

fs, catalog = get_s3_filesystem(), get_catalog()

if "--reset" in sys.argv:
    for name in (SESSIONS, CHANNEL_METRICS, REJECTED_FILES):
        if catalog.table_exists(name):
            catalog.purge_table(name)
    print("tables dropped\n")


def counts() -> str:
    parts = []
    for name in (SESSIONS, CHANNEL_METRICS, REJECTED_FILES):
        t = catalog.load_table(name)
        parts.append(f"{name.split('.')[1]}={t.scan().to_arrow().num_rows}")
    return "  ".join(parts)


# One run per landing day, like the daily schedule of the Airflow DAG.
for i in range(5):
    day = date(2026, 11, 16) + timedelta(days=i)
    keys = list_landing(fs, "raw", day)
    result = ingest(keys, run_id=f"manual__{day.isoformat()}")
    print(f"run {day}: {len(keys):>2} files -> {result!s:<42} | {counts()}")

print("\n--- BACKFILL of 2026-11-16 and 2026-11-18 (same files again) ---")
for day in (date(2026, 11, 16), date(2026, 11, 18)):
    result = ingest(list_landing(fs, "raw", day), run_id=f"backfill__{day.isoformat()}")
    print(f"run {day}: {result!s:<54} | {counts()}")

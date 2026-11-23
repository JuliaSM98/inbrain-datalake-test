"""STEP 2c - Hidden partitioning, partition pruning, metadata-only deletes, partition evolution.

Creates a NEW toy table `toy.sessions` partitioned by days(recorded_at).
Run:  uv run python playground/step2c_partitioning.py   (it recreates the table every time)
"""

from datetime import datetime

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import DoubleType, NestedField, StringType, TimestamptzType

from neural_lakehouse.lakehouse.catalog import get_catalog

catalog = get_catalog()
catalog.create_namespace_if_not_exists("toy")
if catalog.table_exists("toy.sessions"):
    catalog.purge_table("toy.sessions")

schema = Schema(
    NestedField(1, "session_id", StringType(), required=False),
    NestedField(2, "recorded_at", TimestamptzType(), required=False),  # stored in UTC
    NestedField(3, "snr_db", DoubleType(), required=False),
)
# HIDDEN PARTITIONING: partition = a TRANSFORM of a real column. No artificial "day" column.
spec = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=DayTransform(), name="recorded_at_day")
)
table = catalog.create_table("toy.sessions", schema=schema, partition_spec=spec)


def sessions(rows: list[tuple[str, str, float]]) -> pa.Table:
    return pa.table(
        {
            "session_id": [r[0] for r in rows],
            "recorded_at": pa.array(
                [datetime.fromisoformat(r[1]) for r in rows], pa.timestamp("us", tz="UTC")
            ),
            "snr_db": [r[2] for r in rows],
        }
    )


def show_files(title: str) -> None:
    print(f"--- {title}")
    files = table.inspect.files().sort_by("file_path").to_pylist()
    for f in files:
        print(
            f"    spec_id={f['spec_id']}  partition={f['partition']}  rows={f['record_count']}"
            f"  .../{'/'.join(f['file_path'].split('/')[-2:])[:45]}..."
        )


def plan(title: str, row_filter: str) -> None:
    """How many data files does the engine need to OPEN for this filter? (pruning)"""
    total = len(list(table.scan().plan_files()))
    planned = len(list(table.scan(row_filter=row_filter).plan_files()))
    n = table.scan(row_filter=row_filter).to_arrow().num_rows
    print(f"    {title:<44} files to open: {planned}/{total}   rows: {n}")


# 1) ONE append with 12 sessions over 4 days -> Iceberg splits it into one file per day ----------
table.append(
    sessions(
        [
            ("S-001", "2026-11-18T09:30:00+00:00", 18.0),
            ("S-002", "2026-11-18T14:00:00+00:00", 21.0),
            ("S-003", "2026-11-18T23:30:00+00:00", 5.0),
            ("S-004", "2026-11-19T09:30:00+00:00", 19.0),
            ("S-005", "2026-11-19T14:00:00+00:00", 22.0),
            ("S-006", "2026-11-19T23:30:00+00:00", 17.0),
            ("S-007", "2026-11-20T09:30:00+00:00", 16.0),
            ("S-008", "2026-11-20T14:00:00+00:00", 4.0),
            ("S-009", "2026-11-20T23:30:00+00:00", 20.0),
            ("S-010", "2026-11-21T09:30:00+00:00", 18.5),
            ("S-011", "2026-11-21T14:00:00+00:00", 19.5),
            ("S-012", "2026-11-21T23:30:00+00:00", 21.5),
        ]
    )
)
show_files("1) after ONE append of 12 rows (4 days)")

# 2) PARTITION PRUNING: the user filters on the REAL column, Iceberg derives the partitions ------
print("\n--- 2) pruning (filters only mention recorded_at / snr_db, never 'recorded_at_day')")
plan("whole table", "True")
plan(
    "one day (19/11)",
    "recorded_at >= '2026-11-19T00:00:00+00:00' and recorded_at < '2026-11-20T00:00:00+00:00'",
)  # noqa: E501
plan("one exact instant", "recorded_at = '2026-11-20T14:00:00+00:00'")
plan("snr_db > 100 (not a partition: min/max)", "snr_db > 100")

# 3) DELETES: aligned with the partition vs. not aligned ----------------------------------------
print("\n--- 3) deletes")
table.delete("recorded_at >= '2026-11-21T00:00:00+00:00'")  # == the whole 21/11 partition
s = table.current_snapshot().summary
print(
    f"    delete whole day 21/11  -> deleted-data-files={s.get('deleted-data-files')}"
    f"  added-data-files={s.get('added-data-files') or '0'}  (metadata-only: nothing rewritten)"
)
table.delete("snr_db < 6")  # 1 bad row inside 18/11 and 1 inside 20/11
s = table.current_snapshot().summary
print(
    f"    delete snr_db < 6       -> deleted-data-files={s.get('deleted-data-files')}"
    f"  added-data-files={s.get('added-data-files') or '0'}  (copy-on-write: files rewritten)"
)
show_files("after deletes")

# 4) PARTITION EVOLUTION: from now on, partition by MONTH (old files are NOT rewritten) ----------
with table.update_spec() as update:
    update.remove_field("recorded_at_day")
    update.add_field("recorded_at", MonthTransform(), "recorded_at_month")
table.append(
    sessions(
        [
            ("S-013", "2026-12-01T10:00:00+00:00", 20.0),
            ("S-014", "2026-12-15T10:00:00+00:00", 19.0),
            ("S-015", "2026-12-28T10:00:00+00:00", 18.0),
        ]
    )
)
print(f"\n    partition specs in metadata: {[(p.spec_id, str(p)) for p in table.specs().values()]}")
show_files("4) after partition evolution day -> month + append of December")
print("\n--- one query across BOTH specs")
plan("from 19/11 onwards", "recorded_at >= '2026-11-19T00:00:00+00:00'")
plan("only December", "recorded_at >= '2026-12-01T00:00:00+00:00'")

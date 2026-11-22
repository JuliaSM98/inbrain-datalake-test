"""EXERCISE (step 1) - written by Júlia.

Goal: write `list_snapshots(table)` that prints ONE LINE PER SNAPSHOT, oldest first, like:

    7001993850724065271  parent=None                 2026-11-22 10:15:03  append  +2 rows
    7762870947747779859  parent=7001993850724065271  2026-11-22 10:15:03  append  +2 rows
    8385931703721315903  parent=7762870947747779859  2026-11-22 10:15:03  append  +2 rows

and finally marks which one is the CURRENT snapshot.

Run (after step1_toy_table.py):  uv run python playground/exercise_list_snapshots.py

HINT 1: a loaded table has a method that returns all its snapshots. Explore it in a REPL:
        `uv run python`, load the table as below, and try `dir(table)` / `table.snapshots()`.
(Ask Claude for HINT 2 and HINT 3 only if you get stuck.)
"""

import datetime

from pyiceberg.table import Table

from neural_lakehouse.lakehouse.catalog import get_catalog


def list_snapshots(table: Table) -> None:
    # Computed ONCE, outside the loop. None if the table has no snapshots yet.
    current = table.current_snapshot()
    current_id = current.snapshot_id if current else None

    # Oldest first. snapshots() follows metadata.json order, which is not guaranteed.
    for snap in sorted(table.snapshots(), key=lambda s: s.timestamp_ms):
        # Iceberg stores epoch milliseconds in UTC -> always explicit tz (ALCOA+ "Contemporaneous").
        ts = datetime.datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=datetime.UTC)
        added = snap.summary.get("added-records", "0")  # FIX: it lives in the *summary*
        marker = "  <- CURRENT" if snap.snapshot_id == current_id else ""
        print(
            f"{snap.snapshot_id}  "
            f"parent={str(snap.parent_snapshot_id):<19}  "  # pad so columns line up
            f"{ts:%Y-%m-%d %H:%M:%S}  "
            f"{snap.summary.operation.value}  "
            f"+{added} rows{marker}"
        )


if __name__ == "__main__":
    list_snapshots(get_catalog().load_table("toy.readings"))

"""STEP 2a - Time travel, tags and rollback.

Scenario: someone deletes data BY MISTAKE. We use snapshots to see the old data,
pin it with a tag, and roll the table back, without losing the audit trail.

Run (needs the toy table):
    uv run python playground/step1_toy_table.py && uv run python playground/step2a_time_travel.py
"""

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

TABLE_DIR = LOCAL_WAREHOUSE_DIR / "toy/readings"


def n_parquet() -> int:
    return len(list((TABLE_DIR / "data").glob("*.parquet")))


def ids(arrow_table) -> list[int]:
    return sorted(arrow_table.column("reading_id").to_pylist())


table = get_catalog().load_table("toy.readings")
good = table.current_snapshot()
print(f"BEFORE: snapshot {good.snapshot_id}, rows {ids(table.scan().to_arrow())}")
print(f"        parquet files on disk: {n_parquet()}\n")

# 1) THE MISTAKE: delete every reading of sensor B -----------------------------
table.delete(delete_filter="sensor == 'B'")
bad = table.current_snapshot()
s = bad.summary
print("AFTER DELETE (a new snapshot, nothing modified in place):")
print(f"  snapshot {bad.snapshot_id}  operation={s.operation.value}")
print(f"  deleted-data-files={s.get('deleted-data-files')}", end="  ")
print(f"added-data-files={s.get('added-data-files')}")
print(f"  rows now: {ids(table.scan().to_arrow())}")
print(f"  parquet files on disk: {n_parquet()}  <- the old ones are STILL there\n")

# 2) TIME TRAVEL by snapshot id ------------------------------------------------
old = table.scan(snapshot_id=good.snapshot_id).to_arrow()
print(f"TIME TRAVEL by id   ({good.snapshot_id}): rows {ids(old)}")

# 3) TIME TRAVEL by timestamp ("how was the table at time T?") ------------------
#    Pick an instant between the good and the bad snapshot.
t = bad.timestamp_ms - 1
snap_at_t = table.snapshot_as_of_timestamp(t)
print(f"TIME TRAVEL by time (ts={t}): resolves to snapshot {snap_at_t.snapshot_id}")
print(f"  rows {ids(table.scan(snapshot_id=snap_at_t.snapshot_id).to_arrow())}\n")

# 4) TAG: give the good snapshot a stable, human name ---------------------------
table.manage_snapshots().create_tag(good.snapshot_id, "before_incident").commit()
print(f"TAGS/BRANCHES (refs): { {k: v.snapshot_id for k, v in table.metadata.refs.items()} }\n")

# 5) ROLLBACK: move "current" back to the good snapshot --------------------------
table.manage_snapshots().rollback_to_snapshot(good.snapshot_id).commit()
print(f"AFTER ROLLBACK: current snapshot {table.current_snapshot().snapshot_id}")
print(f"  rows now: {ids(table.scan().to_arrow())}")
print(f"  snapshots still in history: {len(table.snapshots())}  <- the bad one is NOT erased")
print("  snapshot-log (what was 'current' over time):")
for entry in table.metadata.snapshot_log:
    tag = (
        "good"
        if entry.snapshot_id == good.snapshot_id
        else ("BAD" if entry.snapshot_id == bad.snapshot_id else "")
    )
    print(f"    {entry.snapshot_id}  {tag}")

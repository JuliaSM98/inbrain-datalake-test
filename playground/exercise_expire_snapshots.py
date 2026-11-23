"""EXERCISE (step 2a) - written by Júlia.  PREDICT FIRST, THEN RUN.

Starting point: the state left by step2a (after the rollback):
    uv run python playground/step1_toy_table.py && uv run python playground/step2a_time_travel.py

Then implement `expire_all_unprotected(table)` and run:
    uv run python playground/exercise_expire_snapshots.py

---------------------------------------------------------------------------------------------
STEP 1 - Write your PREDICTIONS here BEFORE running (don't change them afterwards!):
    a) How many snapshots will remain? The ones with tag and the current, the rest will be expired. So 2
    b) Will the tag 'before_incident' still exist?              -> Yes, it is a ref to the good snapshot, which is protected
    c) How many data files does the current snapshot reference? -> It should be 6 (one for each append and delete)
    d) How many .parquet files will exist ON DISK?              -> The same number of files that in the good snapshot, so 6
    e) Time travel to the BAD snapshot id afterwards: what happens? -> I think it will go to the bad snapshot well, because it has a tag
---------------------------------------------------------------------------------------------

HINT 1: maintenance operations live under `table.maintenance`. Explore it with dir().
        Like `manage_snapshots()`, it is a builder: you chain calls and finish with `.commit()`.
(Ask Claude for HINT 2 if you get stuck.)
"""

import datetime

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

TABLE_DIR = LOCAL_WAREHOUSE_DIR / "toy/readings"


def expire_all_unprotected(table) -> None:
    """Expire every snapshot older than *now* that is not protected."""
    # FIX: expire lives under `table.maintenance` (not manage_snapshots), and the method is
    # `older_than(<datetime>)`. Protected snapshots (heads of branches + tagged) are skipped.
    now = datetime.datetime.now(tz=datetime.UTC)
    table.maintenance.expire_snapshots().older_than(now).commit()


if __name__ == "__main__":
    table = get_catalog().load_table("toy.readings")
    all_ids_before = [s.snapshot_id for s in table.snapshots()]
    # The BAD snapshot = the one created by the delete (operation "overwrite").
    bad_id = next(
        s.snapshot_id for s in table.snapshots() if s.summary.operation.value == "overwrite"
    )
    print(f"BEFORE: {len(all_ids_before)} snapshots, refs={list(table.metadata.refs)}")

    expire_all_unprotected(table)

    table = get_catalog().load_table("toy.readings")  # reload: read the new metadata.json
    print(f"AFTER:  a) snapshots remaining = {len(table.snapshots())}")
    print(f"        b) refs                = {list(table.metadata.refs)}")
    print(f"        c) files referenced    = {table.inspect.files().num_rows}")
    print(f"        d) .parquet on disk    = {len(list((TABLE_DIR / 'data').glob('*.parquet')))}")

    expired = [i for i in all_ids_before if table.snapshot_by_id(i) is None]
    print(f"        e) expired ids = {expired}")
    # Here catching is OK: we EXPECT a failure and we report it (we don't hide it).
    print(f"           time travel to BAD snapshot {bad_id}:")
    try:
        rows = table.scan(snapshot_id=bad_id).to_arrow().num_rows
        print(f"           -> no error, returned {rows} rows")
    except Exception as e:
        print(f"           -> {type(e).__name__}: {e}")

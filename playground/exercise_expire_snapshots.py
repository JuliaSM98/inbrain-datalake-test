"""EXERCISE (step 2a) - written by Júlia.  PREDICT FIRST, THEN RUN.

Starting point: the state left by step2a (after the rollback):
    uv run python playground/step1_toy_table.py && uv run python playground/step2a_time_travel.py

Then implement `expire_all_unprotected(table)` and run:
    uv run python playground/exercise_expire_snapshots.py

---------------------------------------------------------------------------------------------
STEP 1 - Write your PREDICTIONS here BEFORE running (don't change them afterwards!):
    a) How many snapshots will remain?                          ->
    b) Will the tag 'before_incident' still exist?              ->
    c) How many data files does the current snapshot reference? ->
    d) How many .parquet files will exist ON DISK?              ->
    e) Time travel to the BAD snapshot id afterwards: what happens? ->
---------------------------------------------------------------------------------------------

HINT 1: maintenance operations live under `table.maintenance`. Explore it with dir().
        Like `manage_snapshots()`, it is a builder: you chain calls and finish with `.commit()`.
(Ask Claude for HINT 2 if you get stuck.)
"""

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

TABLE_DIR = LOCAL_WAREHOUSE_DIR / "toy/readings"


def expire_all_unprotected(table) -> None:
    """Expire every snapshot older than *now* that is not protected."""
    # TODO(Júlia): your code here
    raise NotImplementedError


if __name__ == "__main__":
    table = get_catalog().load_table("toy.readings")
    all_ids_before = [s.snapshot_id for s in table.snapshots()]
    print(f"BEFORE: {len(all_ids_before)} snapshots, refs={list(table.metadata.refs)}")

    expire_all_unprotected(table)

    table = get_catalog().load_table("toy.readings")  # reload: read the new metadata.json
    print(f"AFTER:  a) snapshots remaining = {len(table.snapshots())}")
    print(f"        b) refs                = {list(table.metadata.refs)}")
    print(f"        c) files referenced    = {table.inspect.files().num_rows}")
    print(f"        d) .parquet on disk    = {len(list((TABLE_DIR / 'data').glob('*.parquet')))}")

    expired = [i for i in all_ids_before if table.snapshot_by_id(i) is None]
    print(f"        e) expired ids = {expired}")
    # TODO(Júlia): try a time travel scan to one of the expired ids and see what happens.

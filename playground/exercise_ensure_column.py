"""EXERCISE (step 2b) - written by Júlia.  An IDEMPOTENT schema migration.

Why: in step 7 Airflow will RETRY tasks and re-run old dates (backfill). Any step of the pipeline,
including "add firmware_version to the table", must be safe to run twice. Running it a second time
must change NOTHING (not even create a new metadata.json).

Implement `ensure_column(table, name, field_type)`:
    - column missing                     -> add it, return "added"
    - column exists with the SAME type   -> do nothing, return "already present"
    - column exists with a DIFFERENT type -> raise ValueError with a clear message
                                            (never "fix" it silently: that's a data contract violation)

Run (starts from the clean toy table):
    uv run python playground/step1_toy_table.py
    uv run python playground/exercise_ensure_column.py

HINT 1: `table.schema()` gives you the current schema. Look for a method on it that FINDS A FIELD
        by name (explore with dir()). What does it do when the name doesn't exist?
HINT 2: to add the column, copy the `with table.update_schema() as update:` pattern from step 2b.
(Ask Claude for HINT 3 if you get stuck.)
"""

from pyiceberg.table import Table
from pyiceberg.types import IcebergType, IntegerType, StringType

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

METADATA_DIR = LOCAL_WAREHOUSE_DIR / "toy/readings/metadata"


def ensure_column(table: Table, name: str, field_type: IcebergType) -> str:
    # TODO(Júlia): your code here
    raise NotImplementedError


def n_metadata() -> int:
    return len(list(METADATA_DIR.glob("*.metadata.json")))


if __name__ == "__main__":
    table = get_catalog().load_table("toy.readings")

    for attempt in (1, 2):
        before = n_metadata()
        result = ensure_column(table, "firmware_version", StringType())
        print(f"attempt {attempt}: {result!r:<20} new metadata.json files: {n_metadata() - before}")

    print("\nwrong type on purpose (should raise ValueError):")
    try:
        ensure_column(table, "firmware_version", IntegerType())
    except ValueError as e:
        print(f"  OK, refused -> {e}")

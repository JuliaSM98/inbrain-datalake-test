"""STEP 2b - Schema evolution: add, rename, illegal type change, drop + re-add ("zombie" test).

Run (starts from the clean toy table):
    uv run python playground/step1_toy_table.py
    uv run python playground/step2b_schema_evolution.py
"""

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.types import LongType, StringType

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

TABLE_DIR = LOCAL_WAREHOUSE_DIR / "toy/readings"


def state(table, title: str) -> None:
    """Print schema (with field ids) + how many data files / snapshots / metadata files exist."""
    sch = table.schema()
    cols = ", ".join(f"{f.field_id}:{f.name}<{f.field_type}>" for f in sch.fields)
    print(f"--- {title}")
    print(f"    schema-id={sch.schema_id}  columns=[{cols}]")
    print(
        f"    parquet={len(list((TABLE_DIR / 'data').glob('*.parquet')))}"
        f"  snapshots={len(table.snapshots())}"
        f"  metadata.json={len(list((TABLE_DIR / 'metadata').glob('*.metadata.json')))}"
    )


def rows(table) -> None:
    for r in table.scan().to_arrow().sort_by("reading_id").to_pylist():
        print(f"      {r}")


table = get_catalog().load_table("toy.readings")
state(table, "0) START")

# 1) ADD COLUMN -----------------------------------------------------------------
with table.update_schema() as update:  # context manager = commits on exit
    update.add_column("firmware_version", StringType(), doc="Device firmware (added later)")
state(table, "1) after ADD firmware_version  (parquet unchanged, NO new snapshot)")
rows(table)

# 2) WRITE NEW DATA WITH THE NEW COLUMN -----------------------------------------
table.append(
    pa.table(
        {
            "reading_id": pa.array([7, 8], pa.int64()),
            "sensor": ["A", "B"],
            "value": [9.5, 15.5],
            "firmware_version": ["1.2.0", "1.2.0"],
        }
    )
)
state(table, "2) after APPEND with firmware_version  (old rows -> None, new rows -> '1.2.0')")
rows(table)

# 3) RENAME COLUMN ---------------------------------------------------------------
with table.update_schema() as update:
    update.rename_column("value", "amplitude_uv")
state(table, "3) after RENAME value -> amplitude_uv  (same field_id 3)")
rows(table)

oldest = sorted((TABLE_DIR / "data").glob("*.parquet"), key=lambda p: p.stat().st_mtime)[0]
raw = pq.read_schema(oldest)
print(f"    Inside the OLDEST parquet ({oldest.name[:20]}...), the column is still called:")
for f in raw:
    print(f"      name={f.name!r:<14} field_id={f.metadata[b'PARQUET:field_id'].decode()}")
print("    -> the file was NOT rewritten; Iceberg matches columns by field_id, not by name.\n")

# 4) ILLEGAL TYPE CHANGE (string -> long): Iceberg refuses it --------------------
print("--- 4) try to change sensor: string -> long")
try:
    with table.update_schema() as update:
        update.update_column("sensor", field_type=LongType())
except Exception as e:  # we EXPECT this to fail, and we report it
    print(f"    REFUSED -> {type(e).__name__}: {e}\n")

# 5) DROP + RE-ADD WITH THE SAME NAME ("zombie data" test) -----------------------
with table.update_schema() as update:
    update.delete_column("sensor")
with table.update_schema() as update:
    update.add_column("sensor", StringType())
state(table, "5) after DROP sensor + ADD sensor  (new field_id -> old 'A'/'B' do NOT come back)")
rows(table)

"""STEP 1 - Iceberg with nothing around it.

Creates a toy table, makes 3 appends and prints what happens after each commit.
Run:  uv run python playground/step1_toy_table.py

It resets `.warehouse/` on every run so you always start from a clean state
(this is a playground, NOT how a real pipeline behaves).
"""

import shutil

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from neural_lakehouse.lakehouse.catalog import LOCAL_WAREHOUSE_DIR, get_catalog

# 0) Clean start ---------------------------------------------------------------
shutil.rmtree(LOCAL_WAREHOUSE_DIR, ignore_errors=True)
catalog = get_catalog()

# 1) Namespace = "database" / "schema" in SQL terms (in Glue: a Glue *database*)
catalog.create_namespace("toy")

# 2) Iceberg schema. Note the `field_id`: Iceberg identifies columns by ID,
#    not by name or position. That is what makes schema evolution safe (step 2).
schema = Schema(
    NestedField(field_id=1, name="reading_id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="sensor", field_type=StringType(), required=False),
    NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
)
# Explicit LOCATION: since step 3 the default warehouse is s3://lakehouse/, but these
# playground tables must stay in the local folder. Each table stores its own location;
# the catalog's "warehouse" is only the DEFAULT for tables created without one.
table = catalog.create_table(
    "toy.readings", schema=schema, location=(LOCAL_WAREHOUSE_DIR / "toy/readings").as_uri()
)
print(f"Created table. Current metadata file:\n  {table.metadata_location}\n")


# 3) Three appends. Each append = one commit = one new snapshot.
def batch(start: int) -> pa.Table:
    """Two rows of fake readings as an in-memory PyArrow table."""
    return pa.table(
        {
            "reading_id": pa.array([start, start + 1], pa.int64()),
            "sensor": ["A", "B"],
            "value": [start * 1.5, start * 2.5],
        }
    )


for i, start in enumerate([1, 3, 5], start=1):
    table.append(batch(start))  # writes Parquet + new metadata, then commits in the catalog
    snap = table.current_snapshot()
    print(f"Append #{i}")
    print(f"  snapshot_id      = {snap.snapshot_id}")
    print(f"  parent_snapshot  = {snap.parent_snapshot_id}")
    print(f"  metadata file    = {table.metadata_location.rsplit('/', 1)[-1]}")
    print(f"  total rows now   = {snap.summary['total-records']}\n")

print("Current table content:")
print(table.scan().to_arrow())

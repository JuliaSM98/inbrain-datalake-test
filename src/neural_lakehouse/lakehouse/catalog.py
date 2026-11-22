"""Single entry point to the Iceberg catalog.

DESIGN DECISION: every part of the project gets the catalog through `get_catalog()`.
Nothing else knows *which* catalog we use. Today it is a local SQLite catalog with a
local-folder warehouse; later it becomes a REST catalog + MinIO, and in real AWS it
would be Glue + S3. Moving between them should only change the properties below
(or configuration), never the pipeline code.

    Step 1 (today):  type=sql,  uri=sqlite:///...,  warehouse=file:///...
    Step 3:          warehouse=s3://lakehouse/  (+ s3.endpoint pointing to MinIO)
    Step 8 (~Glue):  type=rest, uri=http://catalog:8181/...
    Real AWS:        type=glue  (warehouse=s3://my-bucket/...)
"""

from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

# Repo root = three levels up from this file (src/neural_lakehouse/lakehouse/catalog.py)
REPO_ROOT = Path(__file__).resolve().parents[3]

# Local, git-ignored folder that holds BOTH the catalog DB and the table files.
LOCAL_WAREHOUSE_DIR = REPO_ROOT / ".warehouse"


def get_catalog(name: str = "local") -> Catalog:
    """Return the Iceberg catalog used by the project."""
    LOCAL_WAREHOUSE_DIR.mkdir(exist_ok=True)
    return load_catalog(
        name,
        **{
            # "sql" = a catalog stored in a relational DB (here SQLite, one file).
            "type": "sql",
            # Where the catalog itself lives: just a table name -> metadata.json pointer.
            "uri": f"sqlite:///{LOCAL_WAREHOUSE_DIR / 'catalog.db'}",
            # Where table files (data/ + metadata/) are written by default.
            "warehouse": LOCAL_WAREHOUSE_DIR.as_uri(),
        },
    )

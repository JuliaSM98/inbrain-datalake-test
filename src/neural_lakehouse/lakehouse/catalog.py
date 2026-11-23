"""Single entry point to the Iceberg catalog.

DESIGN DECISION: every part of the project gets the catalog through `get_catalog()`.
Nothing else knows *which* catalog or storage we use. Moving between them only changes
configuration (`.env` / environment variables), never the pipeline code.

    Step 1-2:        type=sql (SQLite),  warehouse=file:///.../.warehouse
    Step 3 (today):  type=sql (SQLite),  warehouse=s3://lakehouse/  + s3.endpoint -> RustFS
    Later (~Glue):   type=rest, uri=http://catalog:8181/...
    Real AWS:        type=glue, warehouse=s3://my-bucket/  (no endpoint, no keys: IAM role)
"""

from pyiceberg.catalog import Catalog, load_catalog

from neural_lakehouse.config import get_settings

# Kept for the playground scripts of steps 1-2 (local-folder tables still live here).
LOCAL_WAREHOUSE_DIR = get_settings().catalog_dir


def get_catalog(name: str = "local") -> Catalog:
    """Return the Iceberg catalog used by the project."""
    s = get_settings()
    s.catalog_dir.mkdir(exist_ok=True)

    props: dict[str, str] = {
        # The CATALOG: a SQLite file that maps table name -> current metadata.json.
        "type": "sql",
        "uri": f"sqlite:///{s.catalog_dir / 'catalog.db'}",
        # The STORAGE: default location for new tables' data/ and metadata/.
        "warehouse": s.warehouse,
    }
    # How to reach S3. Only the values that are set are passed: on real AWS there is no
    # endpoint and no keys (the SDK uses the IAM role), so these would all be absent.
    s3_props = {
        "s3.endpoint": s.s3_endpoint,
        "s3.access-key-id": s.s3_access_key,
        "s3.secret-access-key": s.s3_secret_key,
        "s3.region": s.s3_region,
    }
    props |= {k: v for k, v in s3_props.items() if v}
    return load_catalog(name, **props)

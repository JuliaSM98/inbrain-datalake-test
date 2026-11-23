"""The ingestion pipeline: what ONE Airflow run will do (step 7).

    ingest(json_keys, run_id)  ->  validate every upload, then write the results to Iceberg.

fs / catalog / now are parameters so tests can use a local catalog and a fixed time.
"""

from collections import Counter
from datetime import UTC, date, datetime

import pyarrow.fs as pafs
from pyiceberg.catalog import Catalog

from neural_lakehouse.lakehouse.catalog import get_catalog
from neural_lakehouse.lakehouse.tables import SESSIONS, create_tables
from neural_lakehouse.lakehouse.writer import load_existing, write_results
from neural_lakehouse.quality.contract import load_contract
from neural_lakehouse.quality.validate import validate_upload
from neural_lakehouse.utils.s3 import get_s3_filesystem


def list_landing(fs: pafs.FileSystem, bucket: str, day: date) -> list[str]:
    """JSON keys that landed in raw on `day` (the JSON = 'upload complete' marker)."""
    selector = pafs.FileSelector(f"{bucket}/landing/{day.isoformat()}", allow_not_found=True)
    return sorted(i.path for i in fs.get_file_info(selector) if i.path.endswith(".json"))


def ingest(
    json_keys: list[str],
    run_id: str,
    fs: pafs.FileSystem | None = None,
    catalog: Catalog | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    fs = fs or get_s3_filesystem()
    catalog = catalog or get_catalog()
    now = now or datetime.now(tz=UTC)

    contract = load_contract()
    tables = create_tables(catalog)  # idempotent
    existing = load_existing(tables[SESSIONS])  # what is ALREADY in the table

    results = []
    for key in json_keys:
        r = validate_upload(fs, key, contract, existing, now)
        if r.status == "accepted":  # so a duplicate later IN THE SAME BATCH is also caught
            existing[r.metadata["session_id"]] = (r.raw_uri, r.sha256)
        results.append(r)

    write_results(tables, results, run_id, now)
    return dict(Counter(r.status for r in results))

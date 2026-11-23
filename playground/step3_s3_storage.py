"""STEP 3 - Same Iceberg, different storage: a table on S3 (RustFS in Docker).

Needs:  docker compose up -d        (RustFS + buckets `raw` and `lakehouse`)
        an existing local toy.readings  (uv run python playground/step1_toy_table.py)
Run:    uv run python playground/step3_s3_storage.py
"""

import pyarrow.fs as pafs

from neural_lakehouse.config import get_settings
from neural_lakehouse.lakehouse.catalog import get_catalog

settings = get_settings()
catalog = get_catalog()

safe = {k: ("***" if "secret" in k else v) for k, v in catalog.properties.items()}
print(f"Catalog properties (secret hidden):\n  {safe}\n")

# 1) Why can't we just COPY the local folder to the bucket? ------------------------------------
local = catalog.load_table("toy.readings")
first_file = local.inspect.files().column("file_path")[0].as_py()
print("1) The LOCAL table's metadata stores ABSOLUTE paths:")
print(f"     metadata.json -> {local.metadata_location}")
print(f"     a data file   -> {first_file}")
print("   Copying the folder to S3 would leave manifests pointing to file:///Users/... -> broken.")
print("   Migrating = rewrite the data into a new table (or a path-rewrite tool, e.g. Spark's")
print("   rewrite_table_path). Here: read the local table and append it into an S3 table.\n")

# 2) Create the S3 table: no `location` -> it goes to the DEFAULT warehouse (s3://lakehouse/) ---
catalog.create_namespace_if_not_exists("toy_s3")
if catalog.table_exists("toy_s3.readings"):
    catalog.purge_table("toy_s3.readings")
remote = catalog.create_table("toy_s3.readings", schema=local.schema())
remote.append(local.scan().to_arrow())
print("2) New table on S3 (same SQLite catalog, different storage):")
print(f"     metadata.json -> {remote.metadata_location}")
print(
    f"     rows={remote.scan().to_arrow().num_rows}  snapshots={len(remote.snapshots())}"
    f"  (local had {len(local.snapshots())}: the HISTORY is not migrated, only the data)\n"
)

# 3) What is physically in the bucket? (talking S3 directly, like `aws s3 ls --recursive`) -------
host = settings.s3_endpoint.split("://", 1)[1]
fs = pafs.S3FileSystem(
    endpoint_override=host,
    scheme="http",
    access_key=settings.s3_access_key,
    secret_key=settings.s3_secret_key,
    region=settings.s3_region,
)
print(
    "3) Objects under s3://lakehouse/toy_s3/ (each 'path' is ONE key, folders are just prefixes):"
)
for info in fs.get_file_info(pafs.FileSelector("lakehouse/toy_s3", recursive=True)):
    if info.type == pafs.FileType.File:
        print(f"     {info.size:>6} B  s3://{info.path}")

# 4) The catalog is shared: it now knows tables in TWO storages ---------------------------------
print("\n4) One catalog, two storages:")
for ns in ("toy", "toy_s3"):
    for ident in catalog.list_tables(ns):
        loc = catalog.load_table(ident).location()
        print(f"     {'.'.join(ident):<18} -> {loc}")

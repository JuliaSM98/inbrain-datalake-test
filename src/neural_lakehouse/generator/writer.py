"""Write uploads to the RAW zone in S3.

    s3://raw/landing/2026-11-16/upload_0001.json
    s3://raw/landing/2026-11-16/upload_0001.parquet

RAW IS IMMUTABLE: if a file already exists we never overwrite it, we skip it.
So running the generator twice changes nothing (idempotent).
"""

import io
import json

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from neural_lakehouse.generator.sessions import Upload


def write_upload(fs: pafs.FileSystem, bucket: str, upload: Upload) -> str:
    prefix = f"{bucket}/landing/{upload.landing_date.isoformat()}/{upload.upload_id}"
    json_key = prefix + ".json"
    parquet_key = prefix + ".parquet"

    if fs.get_file_info(json_key).type != pafs.FileType.NotFound:
        return "skipped"  # already in raw: never overwrite

    # Signal file -> Parquet bytes (one column per channel).
    columns = {f"ch_{c}": upload.signal[:, c] for c in range(upload.signal.shape[1])}
    buffer = io.BytesIO()
    pq.write_table(pa.table(columns), buffer)
    parquet_bytes = buffer.getvalue()
    if upload.defect == "corrupt_file":
        parquet_bytes = parquet_bytes[: len(parquet_bytes) // 2]

    # Parquet first, JSON last: when the JSON exists, the upload is complete.
    with fs.open_output_stream(parquet_key) as f:
        f.write(parquet_bytes)
    with fs.open_output_stream(json_key) as f:
        f.write(json.dumps(upload.metadata, indent=2).encode())
    return "written"

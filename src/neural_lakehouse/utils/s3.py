"""One place that knows how to reach S3 (RustFS locally, AWS in production)."""

import pyarrow.fs as pafs

from neural_lakehouse.config import Settings, get_settings


def get_s3_filesystem(settings: Settings | None = None) -> pafs.S3FileSystem:
    """PyArrow S3 filesystem configured from settings.

    Locally: endpoint http://localhost:9000 + static keys from `.env`.
    On AWS: no endpoint and no keys -> the SDK uses AWS's endpoint and the IAM role.
    """
    s = settings or get_settings()
    kwargs: dict = {"region": s.s3_region}
    if s.s3_endpoint:
        scheme, host = s.s3_endpoint.split("://", 1)
        kwargs |= {"endpoint_override": host, "scheme": scheme}
    if s.s3_access_key and s.s3_secret_key:
        kwargs |= {"access_key": s.s3_access_key, "secret_key": s.s3_secret_key}
    return pafs.S3FileSystem(**kwargs)

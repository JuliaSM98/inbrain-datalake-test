"""Project configuration, read from environment variables (and `.env` for local development).

Secrets never live in code. Locally they come from `.env` (git-ignored). In a real deployment
they would come from the orchestrator (Airflow Connections) or a secret manager (AWS Secrets
Manager); on AWS, S3 credentials would come from an IAM role and no keys would be set at all.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Repo root = two levels up from this file (src/neural_lakehouse/config.py)
REPO_ROOT = Path(__file__).resolve().parents[2]

# Load `.env` if present. Real environment variables WIN over `.env` (override=False),
# so Docker/Airflow can inject their own values.
load_dotenv(REPO_ROOT / ".env", override=False)


@dataclass(frozen=True)
class Settings:
    # Where the SQL catalog DB lives (local file, git-ignored).
    catalog_dir: Path
    # Default location for NEW tables: "s3://lakehouse/" or a file:// URI.
    warehouse: str
    # S3-compatible endpoint. None = real AWS (the SDK resolves the endpoint itself).
    s3_endpoint: str | None
    s3_access_key: str | None
    s3_secret_key: str | None
    s3_region: str


def get_settings() -> Settings:
    catalog_dir = REPO_ROOT / ".warehouse"
    return Settings(
        catalog_dir=catalog_dir,
        warehouse=os.getenv("LAKEHOUSE_WAREHOUSE") or catalog_dir.as_uri(),
        s3_endpoint=os.getenv("S3_ENDPOINT") or None,
        s3_access_key=os.getenv("S3_ACCESS_KEY") or None,
        s3_secret_key=os.getenv("S3_SECRET_KEY") or None,
        s3_region=os.getenv("S3_REGION", "us-east-1"),
    )

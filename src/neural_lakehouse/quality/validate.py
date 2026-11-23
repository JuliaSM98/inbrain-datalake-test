"""Validate ONE upload of the raw zone against the contract.

Result status:
  accepted     -> goes to standardised (step 6)
  quarantined  -> goes to quarantine.rejected_files, with every violation (step 6)
  skipped      -> this exact file was already ingested (retry / backfill): do nothing
"""

import json
from dataclasses import dataclass, field
from datetime import datetime

import pyarrow.fs as pafs

from neural_lakehouse.quality import checks
from neural_lakehouse.quality.checks import Violation
from neural_lakehouse.utils.hashing import sha256_of


@dataclass
class ValidationResult:
    json_key: str  # raw/landing/2026-11-16/upload_0001.json
    raw_uri: str  # s3://raw/landing/2026-11-16/upload_0001.parquet  -> sessions.raw_uri
    contract_version: str  # which version of the rules was applied (traceability)
    status: str = "quarantined"
    sha256: str | None = None  # fingerprint of the signal file  -> sessions.source_sha256
    metadata: dict = field(default_factory=dict)
    violations: list[Violation] = field(default_factory=list)
    channel_flags: list[str] = field(default_factory=list)  # -> channel_metrics.quality_flag
    drift_fields: list[str] = field(default_factory=list)  # unknown fields: warn, not reject


def validate_upload(
    fs: pafs.FileSystem,
    json_key: str,
    contract: dict,
    existing: dict[str, tuple[str, str]],
    now: datetime,
) -> ValidationResult:
    parquet_key = json_key.removesuffix(".json") + ".parquet"
    result = ValidationResult(json_key, f"s3://{parquet_key}", contract["version"])

    # 1) Read the JSON. If it is not a valid JSON object, nothing else can be checked.
    with fs.open_input_stream(json_key) as f:
        text = f.read()
    try:
        result.metadata = json.loads(text)
    except json.JSONDecodeError as e:
        owner = contract["owners"]["metadata"]
        result.violations.append(Violation("json_readable", f"invalid JSON: {e}", owner))
        return result
    m = result.metadata

    # 2) Metadata and channel checks: ALL of them always run.
    result.violations += checks.check_required_fields(m, contract)
    result.violations += checks.check_allowed_values(m, contract)
    result.violations += checks.check_not_in_future(m, contract, now)
    result.violations += checks.check_n_channels(m, contract)
    result.channel_flags, quality_problems = checks.check_channel_quality(m, contract)
    result.violations += quality_problems
    result.drift_fields = checks.unknown_fields(m, contract)

    # 3) Signal file: is it readable? And its fingerprint (if it exists at all).
    result.violations += checks.check_signal_file(fs, parquet_key, contract)
    if fs.get_file_info(parquet_key).type != pafs.FileType.NotFound:
        result.sha256 = sha256_of(fs, parquet_key)

    # 4) Uniqueness: needs a session_id and a fingerprint.
    if isinstance(m.get("session_id"), str) and result.sha256:
        decision, problems = checks.check_uniqueness(
            m["session_id"], result.raw_uri, result.sha256, existing, contract
        )
        if decision == "skip":
            result.status = "skipped"  # this exact file is already ingested
            return result
        result.violations += problems

    result.status = "quarantined" if result.violations else "accepted"
    return result

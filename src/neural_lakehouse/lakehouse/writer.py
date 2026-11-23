"""Write validation results to the Iceberg tables, IDEMPOTENTLY.

Iceberg commits are atomic PER TABLE, not across tables. So the ORDER matters:
  1. channel_metrics  OVERWRITE the rows of these sessions   (safe to repeat)
  2. sessions         APPEND  <- written LAST: it is the "marker" the uniqueness check reads
  3. quarantine       OVERWRITE the rows of these files      (safe to repeat)
If the job dies between 1 and 2, the retry sees the sessions as "new" and step 1 REPLACES
their channel rows instead of duplicating them.

One commit per table per run (not one per file): fewer snapshots, fewer tiny files.
"""

from datetime import datetime

import pyarrow as pa
from pyiceberg.expressions import In
from pyiceberg.table import Table

from neural_lakehouse.lakehouse.tables import (
    CHANNEL_METRICS,
    CHANNEL_METRICS_SCHEMA,
    REJECTED_FILES,
    REJECTED_FILES_SCHEMA,
    SESSIONS,
    SESSIONS_SCHEMA,
)
from neural_lakehouse.quality.checks import parse_timestamp
from neural_lakehouse.quality.validate import ValidationResult


def load_existing(sessions: Table) -> dict[str, tuple[str, str]]:
    """{session_id: (raw_uri, source_sha256)} of the sessions already in the table."""
    t = sessions.scan(selected_fields=("session_id", "raw_uri", "source_sha256")).to_arrow()
    return {
        sid: (uri, sha)
        for sid, uri, sha in zip(
            t["session_id"].to_pylist(),
            t["raw_uri"].to_pylist(),
            t["source_sha256"].to_pylist(),
            strict=True,
        )
    }


def write_results(
    tables: dict[str, Table], results: list[ValidationResult], run_id: str, now: datetime
) -> None:
    accepted = [r for r in results if r.status == "accepted"]
    rejected = [r for r in results if r.status == "quarantined"]

    if accepted:
        session_rows, channel_rows = [], []
        for r in accepted:
            m = r.metadata
            recorded_at = parse_timestamp(m["recorded_at"])
            session_rows.append(
                {
                    "session_id": m["session_id"],
                    "subject_code": m["subject_code"],
                    "study_id": m["study_id"],
                    "phase": m["phase"],
                    "recorded_at": recorded_at,
                    "device_id": m["device_id"],
                    "sampling_rate_hz": m["sampling_rate_hz"],
                    "n_channels": m["n_channels"],
                    "raw_uri": r.raw_uri,
                    "source_sha256": r.sha256,
                    "contract_version": r.contract_version,
                    "ingested_at": now,
                    "run_id": run_id,
                }
            )
            # Flatten: the JSON list "channels" -> one row per electrode.
            for ch, flag in zip(m["channels"], r.channel_flags, strict=True):
                channel_rows.append(
                    {
                        "session_id": m["session_id"],
                        "channel": ch["channel"],
                        "impedance_kohm": ch["impedance_kohm"],
                        "snr_db": ch["snr_db"],
                        "quality_flag": flag,
                        "recorded_at": recorded_at,
                        "ingested_at": now,
                        "run_id": run_id,
                    }
                )
        session_ids = [row["session_id"] for row in session_rows]

        # 1) channel_metrics: REPLACE the rows of these sessions (delete + insert, one commit).
        tables[CHANNEL_METRICS].overwrite(
            pa.Table.from_pylist(channel_rows, schema=CHANNEL_METRICS_SCHEMA.as_arrow()),
            overwrite_filter=In("session_id", session_ids),
        )
        # 2) sessions LAST (the marker).
        tables[SESSIONS].append(
            pa.Table.from_pylist(session_rows, schema=SESSIONS_SCHEMA.as_arrow())
        )

    if rejected:
        rejected_rows = []
        for r in rejected:
            session_id = r.metadata.get("session_id")
            for v in r.violations:  # one row per failed rule
                rejected_rows.append(
                    {
                        "raw_uri": r.raw_uri,
                        "session_id": session_id if isinstance(session_id, str) else None,
                        "source_sha256": r.sha256,
                        "check_name": v.check_name,
                        "reason": v.reason,
                        "owner": v.owner,
                        "contract_version": r.contract_version,
                        "rejected_at": now,
                        "run_id": run_id,
                    }
                )
        # 3) quarantine: REPLACE the rows of these files -> reprocessing doesn't duplicate them.
        tables[REJECTED_FILES].overwrite(
            pa.Table.from_pylist(rejected_rows, schema=REJECTED_FILES_SCHEMA.as_arrow()),
            overwrite_filter=In("raw_uri", sorted({r.raw_uri for r in rejected})),
        )

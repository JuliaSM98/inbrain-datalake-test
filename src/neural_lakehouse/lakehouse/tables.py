"""The Iceberg tables of the lakehouse: schemas, partitioning and (idempotent) creation.

    standardised.sessions          one row per ACCEPTED session
    standardised.channel_metrics   one row per electrode of an accepted session
    quarantine.rejected_files      one row per FAILED RULE of a rejected upload

Partitioning: months(recorded_at), not days. Each row is a few hundred bytes, so daily
partitions would create many tiny files (over-partitioning, step 2c).
"""

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

SESSIONS = "standardised.sessions"
CHANNEL_METRICS = "standardised.channel_metrics"
REJECTED_FILES = "quarantine.rejected_files"

SESSIONS_SCHEMA = Schema(
    NestedField(1, "session_id", StringType(), required=True),
    NestedField(2, "subject_code", StringType(), required=False),
    NestedField(3, "study_id", StringType(), required=False),
    NestedField(4, "phase", StringType(), required=False),
    NestedField(5, "recorded_at", TimestamptzType(), required=False),
    NestedField(6, "device_id", StringType(), required=False),
    NestedField(7, "sampling_rate_hz", IntegerType(), required=False),
    NestedField(8, "n_channels", IntegerType(), required=False),
    # Lineage / audit columns
    NestedField(9, "raw_uri", StringType(), required=False),  # WHERE the signal file is
    NestedField(10, "source_sha256", StringType(), required=False),  # WHAT it contains
    NestedField(11, "contract_version", StringType(), required=False),  # rules applied
    NestedField(12, "ingested_at", TimestamptzType(), required=False),  # when it entered
    NestedField(13, "run_id", StringType(), required=False),  # which run wrote it
)

CHANNEL_METRICS_SCHEMA = Schema(
    NestedField(1, "session_id", StringType(), required=True),
    NestedField(2, "channel", IntegerType(), required=False),
    NestedField(3, "impedance_kohm", DoubleType(), required=False),
    NestedField(4, "snr_db", DoubleType(), required=False),
    NestedField(5, "quality_flag", StringType(), required=False),  # ok / warning / fail
    NestedField(6, "recorded_at", TimestamptzType(), required=False),  # for partitioning
    NestedField(7, "ingested_at", TimestamptzType(), required=False),
    NestedField(8, "run_id", StringType(), required=False),
)

REJECTED_FILES_SCHEMA = Schema(
    NestedField(1, "raw_uri", StringType(), required=True),
    NestedField(2, "session_id", StringType(), required=False),  # may be missing/unreadable
    NestedField(3, "source_sha256", StringType(), required=False),  # None if file missing
    NestedField(4, "check_name", StringType(), required=False),
    NestedField(5, "reason", StringType(), required=False),
    NestedField(6, "owner", StringType(), required=False),
    NestedField(7, "contract_version", StringType(), required=False),
    NestedField(8, "rejected_at", TimestamptzType(), required=False),
    NestedField(9, "run_id", StringType(), required=False),
)


def _by_month(schema: Schema) -> PartitionSpec:
    source_id = schema.find_field("recorded_at").field_id
    return PartitionSpec(
        PartitionField(source_id, 1000, MonthTransform(), name="recorded_at_month")
    )


def create_tables(catalog: Catalog) -> dict[str, Table]:
    """Create namespaces and tables if they don't exist yet. Safe to run many times."""
    for namespace in ("standardised", "quarantine"):
        catalog.create_namespace_if_not_exists(namespace)
    return {
        SESSIONS: catalog.create_table_if_not_exists(
            SESSIONS, schema=SESSIONS_SCHEMA, partition_spec=_by_month(SESSIONS_SCHEMA)
        ),
        CHANNEL_METRICS: catalog.create_table_if_not_exists(
            CHANNEL_METRICS,
            schema=CHANNEL_METRICS_SCHEMA,
            partition_spec=_by_month(CHANNEL_METRICS_SCHEMA),
        ),
        REJECTED_FILES: catalog.create_table_if_not_exists(
            REJECTED_FILES, schema=REJECTED_FILES_SCHEMA
        ),
    }

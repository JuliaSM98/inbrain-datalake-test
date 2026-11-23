"""Idempotency of the whole pipeline, on a LOCAL catalog + local folder (no S3, no Docker)."""

from datetime import UTC, date, datetime

import pyarrow.fs as pafs
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from neural_lakehouse.generator.sessions import generate_uploads
from neural_lakehouse.generator.writer import write_upload
from neural_lakehouse.lakehouse.tables import CHANNEL_METRICS, REJECTED_FILES, SESSIONS
from neural_lakehouse.pipeline import ingest, list_landing

DAY = date(2026, 11, 16)
NOW = datetime(2026, 11, 23, tzinfo=UTC)


@pytest.fixture
def env(tmp_path):
    """A local raw zone with 12 uploads of one day (2 of them bad) + an empty local catalog."""
    fs = pafs.LocalFileSystem()
    raw = tmp_path / "raw"
    (raw / "landing" / DAY.isoformat()).mkdir(parents=True)  # local disk needs folders
    for upload in generate_uploads(n=12, days=1, start=DAY, bad_ratio=0.2):
        write_upload(fs, str(raw), upload)
    catalog = load_catalog(
        "test",
        type="sql",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=(tmp_path / "warehouse").as_uri(),
    )
    return fs, catalog, list_landing(fs, str(raw), DAY)


def rows(catalog, name):
    return catalog.load_table(name).scan().to_arrow().num_rows


def test_running_twice_changes_nothing(env):
    fs, catalog, keys = env
    first = ingest(keys, "run-1", fs=fs, catalog=catalog, now=NOW)
    before = {n: rows(catalog, n) for n in (SESSIONS, CHANNEL_METRICS, REJECTED_FILES)}

    second = ingest(keys, "run-2", fs=fs, catalog=catalog, now=NOW)
    after = {n: rows(catalog, n) for n in (SESSIONS, CHANNEL_METRICS, REJECTED_FILES)}

    assert first["accepted"] == before[SESSIONS] and first["quarantined"] >= 1
    assert second.get("accepted", 0) == 0  # nothing new the second time
    assert second["skipped"] == first["accepted"]
    assert after == before  # not a single row more, not even in quarantine


def test_crash_between_the_two_tables_then_retry(env, monkeypatch):
    fs, catalog, keys = env

    # Simulate the process dying right before the sessions append (after channel_metrics).
    def crash(self, *args, **kwargs):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(Table, "append", crash)
    with pytest.raises(RuntimeError):
        ingest(keys, "run-1", fs=fs, catalog=catalog, now=NOW)
    assert rows(catalog, SESSIONS) == 0  # the marker was never written...
    written_channels = rows(catalog, CHANNEL_METRICS)
    assert written_channels > 0  # ...but channel_metrics was

    monkeypatch.undo()  # the "machine" is back: Airflow retries the task
    result = ingest(keys, "run-1-retry", fs=fs, catalog=catalog, now=NOW)

    assert rows(catalog, SESSIONS) == result["accepted"]
    assert rows(catalog, CHANNEL_METRICS) == written_channels  # REPLACED, not duplicated
    assert rows(catalog, CHANNEL_METRICS) == 8 * rows(catalog, SESSIONS)

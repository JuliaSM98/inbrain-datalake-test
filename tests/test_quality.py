"""Tests for the quality checks. The limits (boundaries) are where bugs hide: test them."""

import copy
from datetime import UTC, datetime

import pytest

from neural_lakehouse.quality import checks
from neural_lakehouse.quality.checks import channel_flag
from neural_lakehouse.quality.contract import load_contract

CONTRACT = load_contract()
RULES = CONTRACT["channel_quality"]
NOW = datetime(2026, 11, 23, tzinfo=UTC)

GOOD = {
    "session_id": "S-000001",
    "subject_code": "SUBJ-0031",
    "study_id": "STUDY-A",
    "phase": "chronic",
    "recorded_at": "2026-11-16T09:53:00Z",
    "device_id": "DEV-01",
    "sampling_rate_hz": 1000,
    "n_channels": 2,
    "channels": [
        {"channel": 0, "impedance_kohm": 379.2, "snr_db": 27.5},
        {"channel": 1, "impedance_kohm": 573.0, "snr_db": 13.7},
    ],
}


def broken(**changes):
    """A copy of GOOD with some fields changed (value None = remove the field)."""
    m = copy.deepcopy(GOOD)
    for key, value in changes.items():
        if value is None:
            del m[key]
        else:
            m[key] = value
    return m


def test_good_metadata_passes_every_check():
    assert checks.check_required_fields(GOOD, CONTRACT) == []
    assert checks.check_allowed_values(GOOD, CONTRACT) == []
    assert checks.check_not_in_future(GOOD, CONTRACT, NOW) == []
    assert checks.check_n_channels(GOOD, CONTRACT) == []
    assert checks.check_channel_quality(GOOD, CONTRACT) == (["ok", "ok"], [])
    assert checks.unknown_fields(GOOD, CONTRACT) == []


def test_missing_and_wrongly_typed_fields():
    problems = checks.check_required_fields(
        broken(subject_code=None, n_channels="eight", recorded_at="yesterday"), CONTRACT
    )
    assert len(problems) == 3  # ALL problems reported, not just the first one
    assert {p.owner for p in problems} == {"device-integration-team"}


def test_bool_is_not_an_integer():
    assert len(checks.check_required_fields(broken(sampling_rate_hz=True), CONTRACT)) == 1


def test_missing_field_does_not_crash_other_checks():
    m = broken(phase=None, recorded_at=None, channels=None)
    assert checks.check_allowed_values(m, CONTRACT) == []
    assert checks.check_not_in_future(m, CONTRACT, NOW) == []
    assert checks.check_channel_quality(m, CONTRACT) == ([], [])


def test_phase_future_date_and_channel_count():
    assert checks.check_allowed_values(broken(phase="subacute"), CONTRACT)[0].check_name == (
        "allowed_values"
    )
    assert len(
        checks.check_not_in_future(broken(recorded_at="2099-01-01T10:00:00Z"), CONTRACT, NOW)
    )
    assert len(checks.check_n_channels(broken(n_channels=8), CONTRACT)) == 1


def test_failed_channels_boundary_more_than_25_percent():
    bad, good = {"channel": 0, "impedance_kohm": 5000, "snr_db": 20}, GOOD["channels"][0]
    two_of_eight = [bad] * 2 + [good] * 6  # 25.0%  -> passes (rule is MORE THAN)
    three_of_eight = [bad] * 3 + [good] * 5  # 37.5% -> quarantine
    assert checks.check_channel_quality(broken(channels=two_of_eight), CONTRACT)[1] == []
    assert len(checks.check_channel_quality(broken(channels=three_of_eight), CONTRACT)[1]) == 1


def test_unknown_field_is_reported_as_drift():
    assert checks.unknown_fields(broken(battery_level=87), CONTRACT) == ["battery_level"]


def ch(impedance, snr):
    return {"channel": 0, "impedance_kohm": impedance, "snr_db": snr}


@pytest.mark.parametrize(
    ("impedance", "expected"),
    [
        (379.2, "ok"),
        (1000, "ok"),  # exactly ok_max -> still ok
        (1000.1, "warning"),
        (2000, "warning"),  # exactly warning_max -> still warning
        (2000.1, "fail"),
        (5000, "fail"),
    ],
)
def test_impedance_alone(impedance, expected):
    assert channel_flag(ch(impedance, snr=25), RULES) == expected  # snr is fine


@pytest.mark.parametrize(
    ("snr", "expected"),
    [
        (27.5, "ok"),
        (10, "ok"),  # exactly ok_min -> still ok
        (9.9, "warning"),
        (6, "warning"),  # exactly warning_min -> still warning
        (5.9, "fail"),
    ],
)
def test_snr_alone(snr, expected):
    assert channel_flag(ch(impedance=300, snr=snr), RULES) == expected  # impedance is fine


@pytest.mark.parametrize(
    ("impedance", "snr", "expected"),
    [
        (300, 3, "fail"),  # impedance ok + snr fail -> fail
        (1500, 25, "warning"),  # impedance warning + snr ok -> warning
        (1500, 3, "fail"),  # warning + fail -> fail
        (5000, 8, "fail"),  # fail + warning -> fail
    ],
)
def test_worst_flag_wins(impedance, snr, expected):
    assert channel_flag(ch(impedance, snr), RULES) == expected


# --- step 5c: uniqueness (Júlia's exercise), signal file, fingerprint --------------------------

URI_1 = "s3://raw/landing/2026-11-16/upload_0001.parquet"
URI_35 = "s3://raw/landing/2026-11-18/upload_0035.parquet"
EXISTING = {"S-000001": (URI_1, "aaa111")}


def test_uniqueness_new_session():
    assert checks.check_uniqueness("S-000002", URI_1, "bbb", EXISTING, CONTRACT) == ("new", [])


def test_uniqueness_same_file_is_a_retry_not_a_duplicate():
    assert checks.check_uniqueness("S-000001", URI_1, "aaa111", EXISTING, CONTRACT) == ("skip", [])


def test_uniqueness_different_file_is_a_duplicate():
    decision, problems = checks.check_uniqueness("S-000001", URI_35, "ccc", EXISTING, CONTRACT)
    assert decision == "duplicate"
    assert len(problems) == 1
    assert problems[0].check_name == "uniqueness"
    assert problems[0].owner == "data-platform-team"
    assert "upload_0001" in problems[0].reason  # says WHICH file already had that session


def test_uniqueness_same_path_different_hash_means_raw_was_modified():
    decision, problems = checks.check_uniqueness("S-000001", URI_1, "zzz999", EXISTING, CONTRACT)
    assert decision == "modified"
    assert len(problems) == 1


def test_signal_file_missing_corrupt_and_good(tmp_path):
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs = pafs.LocalFileSystem()
    good, corrupt = tmp_path / "good.parquet", tmp_path / "corrupt.parquet"
    pq.write_table(pa.table({"ch_0": [1.0, 2.0]}), good)
    corrupt.write_bytes(good.read_bytes()[:20])  # cut -> footer lost

    assert checks.check_signal_file(fs, str(good), CONTRACT) == []
    assert "unreadable" in checks.check_signal_file(fs, str(corrupt), CONTRACT)[0].reason
    assert "not found" in checks.check_signal_file(fs, str(tmp_path / "nope"), CONTRACT)[0].reason


def test_sha256_known_value_and_sensitivity(tmp_path):
    import pyarrow.fs as pafs

    from neural_lakehouse.utils.hashing import sha256_of

    fs, f = pafs.LocalFileSystem(), tmp_path / "x.bin"
    f.write_bytes(b"hello")
    hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert sha256_of(fs, str(f)) == hello
    f.write_bytes(b"hellO")  # ONE byte changed -> completely different fingerprint
    assert sha256_of(fs, str(f)) != hello

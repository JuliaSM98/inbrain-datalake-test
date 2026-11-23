"""Checks that apply the data contract to one upload.

Each check looks at ONE rule and returns a list of Violations (empty list = rule passed).
All checks always run, so a bad upload reports ALL its problems at once.
"""

from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq


@dataclass
class Violation:
    check_name: str  # which rule failed          -> quarantine.check_name
    reason: str  # human-readable explanation     -> quarantine.reason
    owner: str  # team to notify                  -> quarantine.owner


# YAML type names -> Python types
PYTHON_TYPES = {"string": str, "integer": int, "list": list, "timestamp": str}


def parse_timestamp(value: str) -> datetime | None:
    """'2026-11-16T09:53:00Z' -> datetime in UTC. None if it is not a valid timestamp."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def check_required_fields(metadata: dict, contract: dict) -> list[Violation]:
    owner = contract["owners"]["metadata"]
    problems = []
    for name, type_name in contract["required_fields"].items():
        if name not in metadata:
            problems.append(Violation("required_fields", f"missing field '{name}'", owner))
            continue
        value = metadata[name]
        # bool is a subclass of int in Python: True must not pass as an integer.
        wrong_type = not isinstance(value, PYTHON_TYPES[type_name]) or isinstance(value, bool)
        if type_name == "timestamp" and not wrong_type:
            wrong_type = parse_timestamp(value) is None
        if wrong_type:
            problems.append(
                Violation("required_fields", f"'{name}' should be {type_name}: {value!r}", owner)
            )
    return problems


def check_allowed_values(metadata: dict, contract: dict) -> list[Violation]:
    owner = contract["owners"]["metadata"]
    problems = []
    for name, allowed in contract["allowed_values"].items():
        if name in metadata and metadata[name] not in allowed:  # missing -> already reported
            problems.append(
                Violation("allowed_values", f"'{name}'={metadata[name]!r} not in {allowed}", owner)
            )
    return problems


def check_not_in_future(metadata: dict, contract: dict, now: datetime) -> list[Violation]:
    recorded_at = parse_timestamp(metadata.get("recorded_at"))
    if recorded_at is None or recorded_at <= now:  # missing/invalid -> already reported
        return []
    reason = f"recorded_at {metadata['recorded_at']} is in the future"
    return [Violation("recorded_at_not_in_future", reason, contract["owners"]["metadata"])]


def check_n_channels(metadata: dict, contract: dict) -> list[Violation]:
    n, channels = metadata.get("n_channels"), metadata.get("channels")
    if not isinstance(n, int) or not isinstance(channels, list) or n == len(channels):
        return []
    reason = f"n_channels={n} but the channels list has {len(channels)} items"
    return [Violation("n_channels_must_match", reason, contract["owners"]["metadata"])]


def check_channel_quality(metadata: dict, contract: dict) -> tuple[list[str], list[Violation]]:
    """Flag every channel, and reject the session if too many channels fail.

    Returns (flags, violations): flags = one "ok"/"warning"/"fail" per channel.
    """
    channels = metadata.get("channels")
    if not isinstance(channels, list) or not channels:
        return [], []
    flags = [channel_flag(ch, contract["channel_quality"]) for ch in channels]
    failed_pct = 100 * flags.count("fail") / len(flags)
    if failed_pct > contract["max_failed_channels_pct"]:  # MORE THAN (>), see the contract
        reason = (
            f"{flags.count('fail')}/{len(flags)} channels fail ({failed_pct:.1f}%) "
            f"> {contract['max_failed_channels_pct']}%"
        )
        owner = contract["owners"]["signal_quality"]
        return flags, [Violation("max_failed_channels_pct", reason, owner)]
    return flags, []


def unknown_fields(metadata: dict, contract: dict) -> list[str]:
    """Fields not in the contract (schema drift). NOT a violation: the contract says 'warn'."""
    known = set(contract["required_fields"]) | set(contract["optional_fields"])
    return sorted(set(metadata) - known)


def check_signal_file(fs: pafs.FileSystem, key: str, contract: dict) -> list[Violation]:
    """The .parquet next to the .json must exist and be a readable Parquet file."""
    owner = contract["owners"]["signal_file"]
    if fs.get_file_info(key).type == pafs.FileType.NotFound:
        return [Violation("signal_file_readable", f"signal file not found: {key}", owner)]
    try:
        with fs.open_input_file(key) as f:
            pq.read_metadata(f)  # reads ONLY the footer (end of the file): cheap
    except pa.ArrowInvalid as e:
        # ONLY "this is not a valid Parquet" -> the FILE is bad -> quarantine.
        # A network error (OSError) is NOT caught: the file isn't bad, so the task must fail
        # and Airflow must retry. Quarantining it would be a false positive.
        return [Violation("signal_file_readable", f"unreadable Parquet: {e}", owner)]
    return []


def check_uniqueness(
    session_id: str,
    raw_uri: str,
    sha256: str,
    existing: dict[str, tuple[str, str]],
    contract: dict,
) -> tuple[str, list[Violation]]:
    """EXERCISE (Júlia): the 4 cases of the uniqueness table (contract section 5).

    existing = sessions ALREADY in standardised.sessions:
               {session_id: (raw_uri, sha256)}   e.g. {"S-000001": ("s3://raw/.../upload_0001.parquet",
                                                                    "a3f5...")}
    Return (decision, violations):
      session_id not in existing                      -> ("new", [])
      same raw_uri AND same sha256                    -> ("skip", [])          retry / backfill
      different raw_uri                               -> ("duplicate", [one Violation])
      same raw_uri but different sha256               -> ("modified", [one Violation])

    The Violations: check_name="uniqueness", a clear `reason` that says which file already had
    that session, owner=contract["owners"]["uniqueness"].

    HINT 1: `if session_id not in existing:` first. Then unpack: `old_uri, old_sha = existing[...]`.
    HINT 2: order matters: check "different raw_uri" before comparing hashes.
    Tests: uv run pytest tests/test_quality.py -v -k uniqueness
    """
    if session_id not in existing:
        return ("new", [])
    owner = contract["owners"]["uniqueness"]
    old_uri, old_sha = existing[session_id]
    if raw_uri == old_uri and sha256 == old_sha:
        return ("skip", [])  # the same file again: retry / backfill
    if raw_uri != old_uri:
        # A duplicate = ANOTHER FILE (the hash may or may not differ: don't claim it does).
        reason = f"session_id {session_id} already ingested from {old_uri}"
        return ("duplicate", [Violation("uniqueness", reason, owner)])
    # Only case left: same path, different hash -> the raw zone was modified.
    reason = f"session_id {session_id}: file {old_uri} changed since it was ingested"
    return ("modified", [Violation("uniqueness", reason, owner)])


def channel_flag(channel: dict, rules: dict) -> str:
    """EXERCISE (Júlia): return "ok", "warning" or "fail" for ONE electrode.

    channel = {"channel": 0, "impedance_kohm": 379.2, "snr_db": 27.5}
    rules   = contract["channel_quality"] =
              {"impedance_kohm": {"ok_max": 1000, "warning_max": 2000},
               "snr_db":         {"ok_min": 10,   "warning_min": 6}}

    Impedance (LOWER is better):  <= ok_max -> ok,  <= warning_max -> warning,  else fail
    SNR       (HIGHER is better): >= ok_min -> ok,  >= warning_min -> warning,  else fail
    The WORST of the two wins: fail > warning > ok.

    HINT 1: first compute two flags (one for impedance, one for SNR) with if/elif/else.
    HINT 2: to pick the worst, you can check "is any of them fail?", then "is any warning?".
    Tests: uv run pytest tests/test_quality.py -v
    """
    impedance = channel["impedance_kohm"]
    snr = channel["snr_db"]
    rules_impedance = rules["impedance_kohm"]
    rules_snr = rules["snr_db"]
    if impedance <= rules_impedance["ok_max"]:
        impedance_flag = "ok"
    elif impedance <= rules_impedance["warning_max"]:
        impedance_flag = "warning"
    else:
        impedance_flag = "fail"

    if snr >= rules_snr["ok_min"]:
        snr_flag = "ok"
    elif snr >= rules_snr["warning_min"]:
        snr_flag = "warning"
    else:
        snr_flag = "fail"

    # The worst of the two flags wins.
    flags = (impedance_flag, snr_flag)
    if "fail" in flags:
        return "fail"
    if "warning" in flags:
        return "warning"
    return "ok"

"""Checks that apply the data contract to one upload.

Each check looks at ONE rule and returns a list of Violations (empty list = rule passed).
All checks always run, so a bad upload reports ALL its problems at once.
"""

from dataclasses import dataclass
from datetime import datetime


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

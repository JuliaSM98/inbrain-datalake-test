"""Synthetic recording sessions, deliberately SIMPLE.

Each upload = what a recording device would send to the raw zone:
  - upload_<n>.json     session metadata + per-channel quality reported by the device
  - upload_<n>.parquet  the "heavy" signal file (random numbers: its content doesn't matter)

Some uploads are broken ON PURPOSE (DEFECTS) to test the quarantine in step 5.
The injected defect is saved apart (ground truth): the pipeline never sees it.
"""

import copy
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import numpy as np

PHASES = ["acute", "semi_chronic", "chronic"]

DEFECTS = [
    "missing_field",  # subject_code is absent
    "bad_phase",  # phase is not one of PHASES
    "future_date",  # recorded_at is in the future
    "bad_channels",  # half of the channels have a very high impedance
    "duplicate",  # a NEW upload that contains a session that already arrived
    "corrupt_file",  # the Parquet file is cut in half -> unreadable
]


@dataclass
class Upload:
    upload_id: str  # file name chosen by the uploader, e.g. "upload_0007"
    landing_date: date  # day the files arrive in raw
    metadata: dict  # -> upload_<n>.json
    signal: np.ndarray  # -> upload_<n>.parquet
    defect: str | None = None  # ground truth (None = good upload)


def make_good_upload(rng: np.random.Generator, number: int, landing_date: date) -> Upload:
    recorded_at = datetime(
        landing_date.year, landing_date.month, landing_date.day, 9, tzinfo=UTC
    ) + timedelta(minutes=int(rng.integers(0, 600)))  # between 09:00 and 19:00 UTC

    channels = []
    for ch in range(8):
        channels.append(
            {
                "channel": ch,
                "impedance_kohm": round(float(rng.uniform(50, 800)), 1),
                "snr_db": round(float(rng.uniform(12, 30)), 1),
            }
        )

    metadata = {
        "session_id": f"S-{number:06d}",
        "subject_code": f"SUBJ-{int(rng.integers(1, 40)):04d}",
        "study_id": "STUDY-A",
        "phase": PHASES[int(rng.integers(0, 3))],
        "recorded_at": recorded_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "device_id": f"DEV-{int(rng.integers(1, 10)):02d}",
        "sampling_rate_hz": 1000,
        "n_channels": 8,
        "channels": channels,
    }
    signal = rng.normal(size=(1000, 8)).astype(np.float32)  # 1000 samples x 8 channels
    return Upload(f"upload_{number:04d}", landing_date, metadata, signal)


def generate_uploads(
    n: int = 60,
    seed: int = 42,
    bad_ratio: float = 0.15,
    start: date = date(2026, 11, 16),
    days: int = 5,
) -> list[Upload]:
    rng = np.random.default_rng(seed)  # ONE seed -> the same uploads every time

    # 1) Good uploads, spread evenly over the days (60 uploads / 5 days = 12 per day).
    uploads = []
    for number in range(1, n + 1):
        landing_date = start + timedelta(days=(number - 1) * days // n)
        uploads.append(make_good_upload(rng, number, landing_date))

    # 2) Break some of them on purpose. Never the first one (a duplicate needs an earlier one).
    n_bad = round(n * bad_ratio)
    bad_positions = rng.choice(range(1, n), size=n_bad, replace=False)
    for i, position in enumerate(sorted(bad_positions)):
        upload = uploads[position]
        defect = DEFECTS[i % len(DEFECTS)]
        upload.defect = defect

        if defect == "missing_field":
            del upload.metadata["subject_code"]
        elif defect == "bad_phase":
            upload.metadata["phase"] = "subacute"
        elif defect == "future_date":
            upload.metadata["recorded_at"] = "2099-01-01T10:00:00Z"
        elif defect == "bad_channels":
            for channel in upload.metadata["channels"][:4]:
                channel["impedance_kohm"] = 5000.0
        elif defect == "duplicate":
            # A copy of the FIRST upload (always a good one): same session_id, new file.
            upload.metadata = copy.deepcopy(uploads[0].metadata)
        elif defect == "corrupt_file":
            pass  # the writer cuts the Parquet file in half

    return uploads

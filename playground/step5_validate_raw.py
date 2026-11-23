"""STEP 5 - Validate every upload in the raw bucket, then do it AGAIN (as a retry/backfill would).

Needs: docker compose up -d   and   uv run python -m neural_lakehouse.generator
Run:   uv run python playground/step5_validate_raw.py

`existing` simulates standardised.sessions: in step 6 it will be the real Iceberg table.
"""

import json
from collections import Counter
from datetime import UTC, datetime

import pyarrow.fs as pafs

from neural_lakehouse.config import REPO_ROOT
from neural_lakehouse.quality.contract import load_contract
from neural_lakehouse.quality.validate import validate_upload
from neural_lakehouse.utils.s3 import get_s3_filesystem

fs = get_s3_filesystem()
contract = load_contract()
now = datetime.now(tz=UTC)

# All the JSON files in raw/landing, in order (= arrival order).
infos = fs.get_file_info(pafs.FileSelector("raw/landing", recursive=True))
json_keys = sorted(i.path for i in infos if i.path.endswith(".json"))

existing: dict[str, tuple[str, str]] = {}  # session_id -> (raw_uri, sha256)


def run(title: str) -> list:
    results = []
    for key in json_keys:
        r = validate_upload(fs, key, contract, existing, now)
        if r.status == "accepted":  # "write" it to the (simulated) sessions table
            existing[r.metadata["session_id"]] = (r.raw_uri, r.sha256)
        results.append(r)
    print(f"\n=== {title}: {dict(Counter(r.status for r in results))}")
    return results


first = run("FIRST RUN")
for r in first:
    if r.status == "quarantined":
        upload = r.json_key.rsplit("/", 1)[-1]
        for v in r.violations:
            print(f"  {upload:<18} {v.check_name:<24} owner={v.owner:<24} {v.reason[:60]}")

# Compare with the ground truth (the pipeline never uses it: only this check does).
truth = json.loads((REPO_ROOT / ".data" / "ground_truth.json").read_text())
expected_bad = {u for u, defect in truth.items() if defect}
got_bad = {
    r.json_key.rsplit("/", 1)[-1].removesuffix(".json") for r in first if r.status != "accepted"
}
print(f"\nground truth bad = {len(expected_bad)} | quarantined = {len(got_bad)}")
print(f"  missed (bad but accepted)        : {sorted(expected_bad - got_bad) or 'none'}")
print(f"  false alarms (good but rejected) : {sorted(got_bad - expected_bad) or 'none'}")

run("SECOND RUN (retry / backfill of the same files)")

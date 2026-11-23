"""Generate synthetic uploads into the raw bucket.

    uv run python -m neural_lakehouse.generator

The ground truth (which upload is bad and why) goes to a local, git-ignored file
.data/ground_truth.json. The pipeline must never read it.
"""

import json
from collections import Counter

from neural_lakehouse.config import REPO_ROOT
from neural_lakehouse.generator.sessions import generate_uploads
from neural_lakehouse.generator.writer import write_upload
from neural_lakehouse.utils.s3 import get_s3_filesystem

uploads = generate_uploads()
fs = get_s3_filesystem()

results = Counter()
for upload in uploads:
    results[write_upload(fs, "raw", upload)] += 1
print(f"uploads: {len(uploads)} -> {dict(results)}")

truth = {u.upload_id: u.defect for u in uploads}
truth_file = REPO_ROOT / ".data" / "ground_truth.json"
truth_file.parent.mkdir(exist_ok=True)
truth_file.write_text(json.dumps(truth, indent=2))
print(f"defects: {dict(Counter(d or 'ok' for d in truth.values()))}")
print(f"ground truth saved in {truth_file.relative_to(REPO_ROOT)}")

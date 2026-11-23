"""Load the data contract (YAML file) into a Python dictionary."""

from pathlib import Path

import yaml

from neural_lakehouse.config import REPO_ROOT

CONTRACT_PATH = REPO_ROOT / "contracts" / "session_contract.yaml"


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

"""Smoke test: proves that the package is installed and pytest is wired up."""

import neural_lakehouse


def test_package_imports():
    assert neural_lakehouse.__version__ == "0.1.0"

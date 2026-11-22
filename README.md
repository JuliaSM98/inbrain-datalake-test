# neural-lakehouse (learning project)

A small lakehouse that ingests **synthetic** neural recording sessions, validates them against a
data contract, stores them in **Apache Iceberg** tables and orchestrates everything with **Airflow**.

> **Honesty note:** this is a learning project. All data is synthetic, quality thresholds are
> illustrative, and AWS services (S3, Glue Data Catalog, Athena) are emulated locally with
> open-source equivalents (MinIO, an Iceberg REST catalog, Trino). Nothing runs on a real AWS account.

Full project spec: [PROYECTO_lakehouse_neural_quality.md](PROYECTO_lakehouse_neural_quality.md) ·
Learning log: [LEARNING_LOG.md](LEARNING_LOG.md) · Cheat sheet: [IDEAS_CLAVE.md](IDEAS_CLAVE.md)

## Quick start

```bash
uv sync            # create .venv and install exact versions from uv.lock
uv run ruff check  # lint
uv run pytest      # tests
```

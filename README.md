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

cp .env.example .env    # then set S3_ACCESS_KEY / S3_SECRET_KEY (local-only values)
docker compose up -d    # S3-compatible storage (RustFS) + buckets `raw` and `lakehouse`
```

| Service | URL | Plays the role of |
|---|---|---|
| RustFS S3 API | http://localhost:9000 | Amazon S3 |
| RustFS console | http://localhost:9001 | S3 console |

> MinIO was the original choice, but its container images are no longer published
> (quay.io returns 401 and the Docker Hub repo is gone), so we use **RustFS** (Apache-2.0),
> pinned to `1.0.6`. Any S3-compatible store works: only `S3_ENDPOINT` changes.

Stop: `docker compose down` (keeps data) · `docker compose down -v` (deletes all objects).

# Learning log

Para repasar antes de la entrevista (miércoles 25/11/2026). Una entrada por paso:
qué aprendimos, decisiones y por qué, errores y cómo se resolvieron.

---

## Paso 0: Preparación del repo (21/11/2026)

**Qué hicimos**
- Proyecto Python con `uv` (`pyproject.toml` + `uv.lock`), *src layout* (`src/neural_lakehouse/`).
- `ruff` (lint) y `pytest` (tests) configurados en `pyproject.toml`; un *smoke test*.
- `.gitignore` (secretos, `.venv`, datos locales), `.env.example` sin secretos.

**Decisiones y por qué**
- **`uv` + `uv.lock`**: reproducibilidad; todos instalan exactamente las mismas versiones.
- **Dependencias por paso**: `dependencies = []` al principio; cada librería entra cuando la aprendemos.
- **Trabajar en la raíz del repo** en vez de una subcarpeta `neural-lakehouse/`.
- **MinIO**: misma imagen que en `mrb-db-hub` (`quay.io/minio/minio`), pero **con la versión fijada**
  (allí se usa `latest`). Se fija en el paso 3.
- **Plan comprimido a 4 días** (entrevista el 25/11): fuera PySpark, casi todo OpenTofu/IAM y CI.
- **Airflow con prioridad alta.**

**Errores y soluciones**
- Aviso `VIRTUAL_ENV=/Users/julia/miniconda3 does not match the project environment`: el entorno
  base de conda está activo en la terminal. Inofensivo: `uv` lo ignora y usa `.venv`. Se puede
  quitar con `conda deactivate` (o `conda config --set auto_activate_base false`).

**Hecho cuando** ✅ `uv run pytest` → 1 passed · `ruff check` → All checks passed · primer commit.

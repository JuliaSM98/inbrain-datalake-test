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

---

## Paso 1: Iceberg sin nada alrededor (22/11/2026)

**Qué hicimos**
- `pyiceberg[pyarrow,sql-sqlite]` 0.12.0. Catálogo **SQL (SQLite)** + warehouse en carpeta local `.warehouse/`.
- `get_catalog()` en `src/neural_lakehouse/lakehouse/catalog.py`: **único punto** que sabe qué catálogo usamos.
- Tabla de juguete `toy.readings` (3 columnas con `field_id`), 3 appends → 3 snapshots.
- Recorrimos la cadena a mano: `catalog.db` → `00003.metadata.json` → `snap-....avro` (manifest list)
  → `...-m0.avro` (manifests) → `.parquet`.
- Demo: borramos `catalog.db` → `NoSuchTableError`, pero los 13 archivos seguían ahí →
  `register_table()` con el último metadata.json → 6 filas y 3 snapshots recuperados.
- Ejercicio (Júlia): `list_snapshots()` en `playground/exercise_list_snapshots.py`.

**Lo que observamos**
- 1 append = 1 Parquet + 1 manifest + 1 manifest list + 1 metadata.json. **Nada se modifica, todo se añade.**
- 6 filas → 13 archivos: *small files problem* → hará falta **compaction**.
- La manifest list del snapshot 3 apunta a los 3 manifests (reutiliza, no copia): un snapshot es una
  **foto completa**, no un diff → time travel barato.
- Los manifests guardan **min/max por columna y archivo** → *file pruning* sin abrir Parquet ni listar S3.
- El catálogo guarda `metadata_location` y `previous_metadata_location`: el commit es un
  **compare-and-swap** del puntero (optimistic concurrency).

**Decisiones y por qué**
- Rutas absolutas calculadas desde la raíz del repo en `get_catalog()` (en vez de `.pyiceberg.yaml`)
  para que funcione desde cualquier directorio y haya un solo sitio que cambiar (→ REST → Glue).
- Columnas `required=False` en la tabla de juguete para no pelearnos con la nulabilidad de PyArrow.

**Errores y soluciones**
- Ejercicio v1: `try/except:` desnudo que convertía cualquier error en `NotImplementedError` →
  **esconde la causa**. Quitado. En v2 el error real (`'Snapshot' object has no attribute 'get'`)
  señaló la línea exacta: `added-records` está en `snap.summary`, no en `snap`.
- Timestamps sin zona: `fromtimestamp(ms/1000)` da hora local (11:47) en vez de UTC (10:47).
  → siempre `tz=datetime.UTC`.
- `summary['operation']` es un enum → `.value` para obtener `"append"`.
- No hay `fastavro` instalado: para leer los Avro de Iceberg usamos la propia API de PyIceberg
  (`snapshot.manifests(io)`, `manifest.fetch_manifest_entry(io)`).

**Respuestas mejoradas (entrevista)**
- *Lose the catalog?* → Los datos no se pierden (archivos autodescriptivos, `register_table`), se pierde
  el **puntero**. Pero el catálogo es la *source of truth* de cuál es el metadata.json vigente
  (puede haber metadata.json de commits fallidos) → necesita backup.

---

## Paso 2a: Time travel, tags, rollback y expire (22-23/11/2026)

**Qué hicimos** (`playground/step2a_time_travel.py`, ejercicio `exercise_expire_snapshots.py`)
- Delete "por error" de `sensor == 'B'` → snapshot `overwrite` con `deleted-data-files=3`,
  `added-data-files=3`; Parquet en disco 3 → 6.
- Time travel por id (`scan(snapshot_id=)`) y por timestamp (`snapshot_as_of_timestamp`).
- Tag `before_incident` sobre el snapshot bueno; rollback → el snapshot malo **sigue en el historial**
  (`snapshot-log`: good → BAD → good) → audit trail completo.
- Ejercicio: `table.maintenance.expire_snapshots().older_than(now).commit()`.

**Resultado del ejercicio vs predicciones de Júlia**
| | Predicción | Real | Por qué |
|---|---|---|---|
| a) snapshots | 2 | **1** | Tras el rollback, `main` y el tag apuntan al **mismo** snapshot |
| b) tag | sigue | **sigue** ✅ | Tags y heads de ramas están **protegidos** |
| c) data files referenciados | 6 | **3** | El snapshot actual solo ve sus 3 Parquet originales: es una foto, no la suma del historial |
| d) Parquet en disco | 6 | **6** ✅ (motivo distinto) | **PyIceberg 0.12 expira solo metadatos, no borra archivos** → 3 *orphans* |
| e) time travel al malo | funciona | **`ValueError: Snapshot not found`** | El tag protegía al bueno, no al malo; sin snapshot en metadata no hay forma de saber qué archivos lo formaban |

**Lecciones**
- **Copy-on-write**: un delete reescribe *cada archivo que contiene alguna fila afectada*, no "un archivo
  por fila". Si el delete coincide con la partición → *metadata-only delete* (no se reescribe nada).
- **Expirar ≠ borrar archivos.** En Spark/Trino `expire_snapshots` también borra los archivos que ya
  nadie referencia; en PyIceberg 0.12 no → hace falta limpieza de **orphan files** (`remove_orphan_files`
  en Spark/Trino). Si no, el almacenamiento crece para siempre.
- Tras expirar, **el time travel a ese snapshot es imposible** aunque los Parquet sigan en disco.
  La retención es decisión de negocio/compliance.
- **Parquet = formato; data file = rol** (archivo referenciado por un manifest). Un Parquet no
  referenciado es un *orphan* y ningún motor lo ve (los motores no listan carpetas).

**Errores y soluciones**
- `table.manage_snapshots().expire_snapshots()` → `AttributeError`. Expire vive en
  `table.maintenance`, y el método es `older_than(datetime)`, no `"now"`.
- Respuesta de entrevista "recover accidental delete" demasiado corta → versión completa: parar el
  daño, localizar snapshot malo, comprobar que el bueno no está expirado, verificar con time travel,
  rollback **solo si no hubo escrituras válidas después** (si no, reinsertar las filas borradas).

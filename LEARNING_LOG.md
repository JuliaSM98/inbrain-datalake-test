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

---

## Paso 2b: Schema evolution (23/11/2026)

**Qué hicimos** (`playground/step2b_schema_evolution.py`, ejercicio `exercise_ensure_column.py`)
- `add_column("firmware_version")` → schema-id 0→1, **0 Parquet nuevos, 0 snapshots nuevos**, solo un
  metadata.json más. Filas viejas → `None`.
- Append con la columna nueva: conviven archivos escritos con 2 esquemas.
- `rename_column("value", "amplitude_uv")` → abrimos el Parquet más antiguo con PyArrow: dentro sigue
  llamándose `value` con `PARQUET:field_id=3`. **Iceberg empareja por field_id, no por nombre.**
- `string → long` → `ValidationError` (solo *type promotions* seguras: int→long, float→double, decimal).
- Drop `sensor` + add `sensor` → nuevo field_id 5 → valores viejos **no** reaparecen (no hay zombis).

**Lecciones**
- Cambiar esquema = solo metadatos (nuevo schema-id); los datos no se tocan.
- Drop de columna **no borra físicamente** los datos (siguen en los Parquet viejos). Para borrado real
  (GDPR) → reescribir archivos + expirar snapshots.
- **Idempotencia** de migraciones: `ensure_column` añade solo si falta, no hace nada si ya está,
  y **falla en voz alta** si el tipo no coincide.

**Errores y soluciones**
- `name in schema` → **siempre False**: `Schema` es un modelo pydantic y `in` itera sus atributos
  como pares `(clave, valor)`. Intento 1 añadía bien, intento 2 petaba con
  `ValueError: Cannot add column, name already exists`. Solución: `name in schema.column_names`.
  Lección: comprobar con un caso que **debería dar True** (no solo el que da False).

---

## Paso 2c: Hidden partitioning y partition evolution (23/11/2026)

**Qué hicimos** (`playground/step2c_partitioning.py`, tabla `toy.sessions`)
- Spec `days(recorded_at)`: 1 append de 12 filas → 4 archivos (uno por día), sin columna "día".
- Pruning filtrando solo por `recorded_at`: 1 día → 1/4 archivos; `snr_db > 100` → 0/4 (min/max).
- Delete de un día entero → `deleted-data-files=1, added=0` (**metadata-only**);
  delete `snr_db < 6` → 2 reescritos (copy-on-write).
- Evolución `day → month`: archivos viejos con `spec_id=0`, nuevos con `spec_id=1`; una consulta
  planifica con ambos. `683` = meses desde 1970-01 (las transforms producen enteros).
- Demo de zona horaria: S-006 (19/11 23:30 UTC = 20/11 00:30 Madrid) está en la partición **UTC 19/11**;
  filtro "20/11 hora de Madrid" abre particiones 19 y 20 y devuelve S-006 y S-007 (correcto).

**Respuestas de diseño de Júlia y corrección**
1. *Over-partitioning*: ❌ el riesgo no es "un archivo muy grande" sino **muchos archivos diminutos**
   (1-3 filas por día). Coste fijo por archivo (abrir, footer, GET a S3, entradas en manifests).
   Con 50-100 sesiones (y aun con miles/día, porque cada fila son ~200 bytes) → **sin partición o
   `months`**. Regla: particionar cuando cada partición tiene cientos de MB/GB.
2. *Zona horaria*: ❌ las transforms de Iceberg sobre `timestamptz` se calculan **siempre en UTC**.
   ✅ el resultado sí es correcto, pero porque **la partición solo decide qué archivos se abren;
   el filtro se aplica a los valores reales**. Particionar nunca cambia resultados, solo coste.
   (En Hive, filtrando por una columna `fecha_dia` mal calculada, el resultado sería **incorrecto**.)
3. *channel_metrics*: 🟡 la intuición (juntar los canales de una sesión) es buena, pero `identity(session_id)`
   = una partición por sesión = over-partitioning. Mejor `bucket(N, session_id)` o **ordenar por
   session_id** (sort order) y dejar que min/max haga el pruning; o añadir `recorded_at` y `months`.

**Errores y soluciones**
- `NotInstalledError: pyiceberg_core needs to be installed` al escribir en tabla particionada →
  añadir extra `pyiceberg[pyiceberg-core]` (transforms en Rust, iceberg-rust).
- `ruff` falló pero el commit pasó porque `| grep` ocultaba el código de salida → commit de arreglo.

---

## Hallazgo antes del Paso 3: MinIO ya no se distribuye como imagen (23/11/2026)
- `quay.io/minio/minio:latest` → **401 UNAUTHORIZED**; `minio/minio` en Docker Hub → **no existe**.
- Consecuencia: el `docker-compose.yml` de `mrb-db-hub` (que usa `quay.io/minio/minio` sin tag)
  **fallará en cualquier máquina que no tenga la imagen en caché** (CI, portátil nuevo).
- Alternativas disponibles hoy (Docker Hub): RustFS `1.0.6` (Apache-2.0), SeaweedFS `4.55`
  (Apache-2.0), Garage `v2.5.0` (AGPL).

---

## Paso 3: Almacenamiento S3 con RustFS (23/11/2026)
- `docker-compose.yml`: `rustfs/rustfs:1.0.6` (versión fijada) + job `create-buckets` con la AWS CLI
  oficial (idempotente: `head-bucket` antes de `mb`). En AWS real los buckets se crean con IaC.
- `config.py` lee `.env` (git-ignored); `get_catalog()` añade `s3.*` solo si existen (en AWS: nada).
- Mismo catálogo SQLite, warehouse `s3://lakehouse/`. Cada tabla guarda **su propia location**.
- Iceberg guarda **rutas absolutas** → mover una tabla = reescribirla (y se pierde el historial).
- `docker compose down -v` → el catálogo queda con un **puntero colgante** (`FileNotFoundError`).
  Inverso del Paso 1: catálogo y almacenamiento se respaldan **juntos**.
- Dos `metadata.json` en una tabla nueva = 2 commits (`create_table` vacía + `append`). Los dos
  `.avro` no son "dos manifests": uno es la **manifest list** y otro el **manifest**.

---

## Paso 4: Generador de subidas sintéticas (23/11/2026)

**Decisión: simplificar.** Primera versión con senoides + ruido gaussiano + SNR objetivo →
demasiado procesamiento de señal para un proyecto cuyo objetivo es *data engineering*.
Versión final:
- El dispositivo **informa** impedancia y SNR por canal en el JSON → la calidad es comparar con umbrales.
- La "señal" es un Parquet de números aleatorios: solo existe para practicar "archivo pesado en S3,
  la tabla guarda URI + hash".
- Archivos `upload_<n>` (nombre del que sube); el `session_id` va **dentro** → los duplicados se
  detectan **por contenido, no por nombre de archivo**.
- 8 canales, 60 subidas, 12 por día (16-20/11), 9 malas con 6 tipos de defecto.
- Raw inmutable: si el JSON ya existe, `skipped` → generador idempotente.

**Lo que se aprendió por el camino (versión descartada)**
- Ejercicio de Júlia, correcto y con tests en verde:
  `P_signal = mean(signal²)`, `P_noise = P_signal / 10^(SNR/10)`, `sigma = sqrt(P_noise)`.
- En disco local hay que crear la carpeta antes de escribir; en S3 no (no hay carpetas, solo keys).

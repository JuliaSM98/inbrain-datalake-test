# Proyecto: Lakehouse de calidad de datos neuronales (S3 + Iceberg + Glue + Snowflake + Airflow)

> **Versión 100 % gratuita:** no requiere cuenta de AWS ni licencias de pago. Todo usa herramientas de código abierto o gratuitas; los servicios de AWS se practican con equivalentes locales y se estudian como concepto (ver sección 13).
>
> Documento pensado para dárselo a Claude Code al empezar el proyecto. Contiene el contexto, los objetivos de aprendizaje, la arquitectura, las tecnologías explicadas desde cero, un plan por fases con criterios de "hecho" y las reglas de cómo quiero que me ayudes.

---

## 0. Instrucciones para Claude Code (LEER PRIMERO)

Soy **Júlia**, ingeniera de software y datos (Python, FastAPI, PostgreSQL/PostGIS, Docker, ETL con Celery, AWS S3 básico). **No tengo experiencia en producción con Iceberg, Glue, Snowflake ni Airflow.** Estoy preparando una entrevista técnica para un puesto de *Senior Data Engineer* en una empresa de neurotecnología médica (grabaciones neuronales, entorno regulado). El objetivo del proyecto es **aprender de verdad y poder explicarlo en una entrevista**, no solo tener código que funcione.

### Cómo quiero que trabajes
1. **Modo profesor, no modo autopiloto.** Antes de escribir código de una pieza nueva, explícame en 5-10 líneas qué es, por qué existe y qué problema resuelve. Después escribe el código.
2. **Pasos pequeños con checkpoint.** Cada fase termina con algo que puedo ejecutar y ver funcionar. No avances a la siguiente sin que yo confirme.
3. **Déjame escribir partes clave.** En cada fase, elige 1-2 funciones o consultas importantes (validación de calidad, la consulta de time travel, el DAG) y propónmelas como ejercicio: yo las escribo, tú las revisas. Dame pistas antes que la solución.
4. **Hazme preguntas de entrevista** al cerrar cada fase (3-5 preguntas, en inglés, con feedback honesto sobre mis respuestas).
5. **Responde en español**, pero mantén los términos técnicos en inglés (snapshot, partition, DAG, schema evolution...) y, al final de cada fase, dame 2-3 frases en inglés que yo pueda decir en la entrevista.
6. **Mantén un archivo `LEARNING_LOG.md`** donde apuntes, al cerrar cada fase: qué aprendimos, decisiones tomadas y por qué, errores que tuvimos y cómo los resolvimos. Lo usaré para repasar.
7. **Verifica la documentación actual.** Las versiones y la sintaxis de Airflow, PyIceberg, Athena y sobre todo Snowflake cambian. Si dudas de un comando, comprueba la documentación oficial en vez de asumir.
8. **Sé honesto sobre límites.** Si algo del proyecto es una simplificación respecto a un sistema real (p. ej., umbrales de calidad inventados), dilo en el código y en el README.

### Reglas de seguridad y coste (no negociables)
- **Todo debe ser gratuito.** No tengo cuenta de AWS ni licencias de ningún programa. Usa solo herramientas de código abierto o gratuitas que corran en mi ordenador (Docker, Python, Airflow open source, MinIO, PyIceberg, DuckDB, Trino, OpenTofu...). **No propongas nada que requiera tarjeta de crédito ni cuenta de pago.** Si una herramienta cambia su licencia o deja de ser gratuita, avísame y propón una alternativa.
- Los servicios de AWS y Microsoft que no se pueden practicar gratis (Glue, Athena, Snowflake sobre S3, Azure...) se estudian con el mapa de la sección 13 y se explican como concepto, sin fingir que se han usado.
- **Nunca** ejecutes comandos que creen recursos de pago en AWS o Snowflake **sin pedirme confirmación explícita** y explicarme el coste esperado.
- **Nunca** pongas credenciales, claves o tokens en el código ni en Git. Usa variables de entorno y un `.env` incluido en `.gitignore`. Incluye un `.env.example` sin secretos.
- **Nada de datos reales de pacientes.** Todo son datos sintéticos generados por código.
- **No hagas `git push`** ni crees repositorios remotos sin que yo lo pida. Commits locales por hito, sí.
- Principio de **mínimo privilegio** en IAM: nada de políticas con `*` sobre todos los recursos salvo en una fase de pruebas, y avisando.
- Al final de cada fase de nube, recuérdame **qué recursos borrar** y dame los comandos.

---

## 1. Resumen del proyecto

**Idea**: construir un pequeño *lakehouse* que ingiere "sesiones de grabación neuronal" simuladas, valida su calidad, las organiza en tablas versionadas y permite consultarlas con distintos motores.

**Qué simula** (inspirado en el tipo de trabajo de una empresa de interfaces cerebro-máquina):
- Llegan archivos de sesiones de grabación (señal multicanal + metadatos de la sesión).
- Un pipeline comprueba la **calidad**: relación señal-ruido (SNR), impedancia de electrodos, completitud y formato de metadatos.
- Los datos que cumplen el **data contract** pasan a tablas Iceberg; los que no, van a una **cuarentena** con el motivo del rechazo y el responsable.
- Las tablas se consultan con **Athena** (AWS) y **Snowflake**, y se puede viajar en el tiempo (versiones anteriores de los datos).
- **Airflow** orquesta todo, con reintentos, programación y reprocesado (backfill).

**Qué NO es**: no es un sistema clínico ni usa fisiología real. Las señales son sintéticas y los umbrales son ilustrativos.

### Objetivos de aprendizaje (lo que debería saber explicar al terminar)
- Qué es un lakehouse y en qué se diferencia de un data lake y de un data warehouse.
- Qué aporta Iceberg sobre "Parquet suelto en S3": snapshots/time travel, ACID, schema evolution, partition evolution, hidden partitioning.
- Cómo se relacionan S3 (almacenamiento), Iceberg (formato de tabla), Glue Data Catalog (catálogo) y los motores (Athena/Spark/Snowflake).
- Cómo se orquesta un pipeline en Airflow: DAG, tareas, dependencias, retries, idempotencia, backfill, scheduling.
- Cómo se implementa calidad de datos: data contracts, validaciones, cuarentena, trazabilidad (lineage) y auditoría (ALCOA+).
- Qué es Snowflake y cómo puede leer tablas Iceberg que viven en S3.
- Buenas prácticas básicas: IaC (Terraform), CI/CD (GitHub Actions), tests, mínimo privilegio, control de costes.

---

## 2. Arquitectura

### 2.1 Visión general
```
                        ┌─────────────────────────────────────────────┐
                        │                  AIRFLOW                    │
                        │   (orquesta, programa, reintenta, alerta)   │
                        └───────────────┬─────────────────────────────┘
                                        │ dispara y coordina
   Generador sintético                  ▼
   (Python/NumPy)  ──►  S3 / MinIO: zona RAW      (archivos tal cual llegan)
                                        │
                                        ▼  validar contrato + calcular métricas
                              ┌─────────┴──────────┐
                              │                    │
                         OK   ▼               KO    ▼
              Iceberg: STANDARDISED      Iceberg: QUARANTINE
              (sessions, channel_metrics)  (rechazos + motivo)
                              │
                              ▼  agregaciones / reglas de negocio
                     Iceberg: CURATED
                    (datasets listos para ML/analítica)
                              │
        ┌─────────────────────┼──────────────────────┐
        ▼                     ▼                      ▼
     Athena            Snowflake (Iceberg      PyIceberg / DuckDB
   (SQL en AWS)        externo, solo lectura)   (Python/notebooks)

   Catálogo: Glue Data Catalog (AWS)  |  en local: catálogo SQL (SQLite) o REST
```

> **En la versión gratuita**: S3 → MinIO, Glue Catalog → catálogo REST de Iceberg, Athena → Trino/DuckDB, Snowflake → concepto + prueba opcional de 30 días (ver sección 13).

### 2.2 Zonas (medallion adaptado)
| Zona | Contenido | Formato | Regla |
|---|---|---|---|
| **raw** | Archivos exactamente como llegan (señal + JSON de metadatos) | Parquet/NPZ + JSON, objetos sueltos en S3 | **Inmutable.** Nunca se modifica ni se borra. |
| **standardised** | Datos validados y en esquema común | Tablas Iceberg | Solo entra lo que cumple el contrato. |
| **quarantine** | Archivos/registros rechazados + motivo | Tabla Iceberg | Se guarda por qué falló, qué check, cuándo y a quién se notifica. |
| **curated** | Datasets listos para consumo (analítica/ML) | Tablas Iceberg versionadas | Documentados y reproducibles (se pueden referenciar por snapshot). |

### 2.3 Decisión de diseño importante
Los **archivos grandes** (la señal completa de una grabación) se quedan como **objetos en S3**. Las tablas guardan **metadatos de la sesión + métricas + la ruta (URI) al archivo**. Esto replica cómo se manejan en la práctica los datos pesados (vídeo, HDF5/NWB): no se meten dentro de la tabla.

---

## 3. Tecnologías explicadas desde cero

### 3.1 Amazon S3 (almacenamiento de objetos)
- Almacén de **objetos** (archivos) organizados en *buckets* con *keys* (rutas). No es un sistema de archivos clásico: no hay carpetas reales, solo prefijos.
- Barato, muy duradero, escalable. Es la base del data lake.
- Los objetos son inmutables: para "cambiar" un archivo se escribe uno nuevo. Esto es clave para entender por qué existe Iceberg.
- Conceptos a dominar: bucket, key/prefix, versioning del bucket, lifecycle rules, cifrado (SSE), políticas de bucket, clases de almacenamiento.

**En local** lo sustituimos por **MinIO**, que expone la misma API que S3.

### 3.2 Parquet (formato de archivo)
- Formato **columnar** comprimido. Leer solo las columnas necesarias es muy rápido. Guarda estadísticas por bloque (min/max) que permiten saltar datos.
- Parquet por sí solo es **solo un archivo**: no sabe qué otros archivos forman una "tabla".

### 3.3 Apache Iceberg (formato de tabla)
**El problema**: con "un montón de Parquet en S3", no hay transacciones (alguien lee mientras otro escribe), no hay versiones, cambiar el esquema es doloroso y listar miles de archivos es lento.

**La solución**: Iceberg añade una **capa de metadatos** (archivos JSON y Avro, guardados junto a los datos) que define qué archivos Parquet componen la tabla en cada momento.

Estructura de una tabla Iceberg:
```
tabla/
  data/        parte-0001.parquet, parte-0002.parquet ...   ← los datos
  metadata/
    v3.metadata.json         ← estado actual de la tabla (esquema, particiones, snapshots)
    snap-123.avro            ← manifest list de un snapshot
    manifest-abc.avro        ← lista de archivos de datos + estadísticas
```
Jerarquía: **catálogo → metadata.json → manifest list → manifests → data files**.

Capacidades clave:
- **Snapshots / time travel**: cada commit crea un snapshot. Puedes consultar la tabla "como estaba" en una fecha o versión.
- **ACID**: los commits son atómicos; los lectores ven siempre un snapshot consistente.
- **Schema evolution**: añadir, renombrar o eliminar columnas sin reescribir los datos (las columnas se identifican por ID, no por nombre/posición).
- **Partition evolution** y **hidden partitioning**: puedes particionar por una transformación (`days(recorded_at)`) sin que el usuario tenga que filtrar por una columna de partición artificial, y cambiar el esquema de particionado más adelante.
- **Row-level operations**: `MERGE`, `UPDATE`, `DELETE` (en motores que lo soportan).
- **Mantenimiento**: compactación de archivos pequeños, expiración de snapshots antiguos, borrado de archivos huérfanos.

Alternativas: **Delta Lake** (Databricks), **Apache Hudi**. Todas resuelven un problema parecido; Iceberg destaca por su apertura y su soporte multi-motor.

### 3.4 AWS Glue (dos cosas distintas con el mismo nombre)
- **Glue Data Catalog**: el **catálogo** (metastore). Sabe qué bases de datos y tablas existen y dónde está el `metadata.json` actual de cada tabla Iceberg. Athena, Spark y Snowflake lo pueden usar.
- **Glue Jobs / ETL**: ejecución de transformaciones (Spark o Python shell), de pago por tiempo de cómputo (DPU-hora).
- **Glue Crawlers**: descubren esquemas automáticamente (en este proyecto los usaremos poco o nada; con Iceberg el esquema ya está en los metadatos).

### 3.5 Amazon Athena (motor SQL serverless)
- Ejecuta SQL directamente sobre datos en S3 sin servidores. Pagas por datos escaneados.
- Soporta tablas Iceberg (crear tablas, insertar, `MERGE`, time travel).
- Usa el Glue Data Catalog como catálogo.
- Ideal para explorar y para validar el proyecto.

### 3.6 Apache Spark (nota)
- Motor de procesamiento distribuido. En AWS se usa vía Glue Jobs o EMR. En este proyecto es **opcional** (Fase 2b): un Glue Job que haga una transformación para entender cómo encaja. Los datos son pequeños, así que no lo necesitamos por volumen, solo por aprendizaje.

### 3.7 Snowflake
- **Data warehouse en la nube** con **almacenamiento y cómputo separados**. El cómputo se ejecuta en *virtual warehouses* (se encienden, se pausan automáticamente y se cobran por uso).
- Conceptos: account, role, warehouse, database, schema, stage.
- **Tablas Iceberg en Snowflake**: Snowflake puede leer tablas Iceberg que viven en tu S3.
  - **External volume**: define a qué ubicación de S3 puede acceder Snowflake (vía un rol IAM con relación de confianza).
  - **Catalog integration**: le dice a Snowflake cuál es el catálogo (Glue, o leer metadatos directamente del almacenamiento).
  - **Iceberg table (gestionada externamente)**: Snowflake la ve y la consulta en **solo lectura**; la escritura sigue en tu pipeline. Hay que **refrescar** para ver nuevos snapshots.
  - Alternativa: tabla Iceberg *gestionada por Snowflake* (Snowflake escribe). No la usaremos, pero hay que saber que existe.
- Se usa cuenta de **prueba gratuita**. Configura el warehouse más pequeño con `AUTO_SUSPEND` corto.

> ⚠️ La sintaxis exacta de external volume / catalog integration / refresh cambia con las versiones. **Consulta siempre la documentación actual de Snowflake** antes de escribirla.

### 3.8 Apache Airflow (orquestador)
- Programa y supervisa **workflows** definidos como código Python.
- Conceptos:
  - **DAG**: grafo acíclico de tareas con dependencias.
  - **Task / Operator**: unidad de trabajo. Se recomienda la **TaskFlow API** (`@task`) para tareas en Python.
  - **Scheduler / Executor / Webserver / Metadata DB**: componentes internos.
  - **Schedule y data intervals**: cada ejecución del DAG procesa un intervalo de tiempo.
  - **Retries, timeouts, SLAs, callbacks** de fallo (alertas).
  - **Catchup y backfill**: reprocesar intervalos pasados.
  - **Idempotencia**: ejecutar la misma tarea dos veces con la misma entrada debe dar el mismo resultado (clave para reintentos y backfill).
  - **Connections y Variables**: para configuración y credenciales (nunca en el código).
  - **XCom**: pasar datos *pequeños* entre tareas (nunca datos grandes).
  - **Dynamic task mapping**: crear una tarea por cada archivo nuevo.
  - **Sensors**: esperar a que llegue un archivo.
  - **Assets/Datasets** (según versión): programación basada en datos.
- Alternativa: **Dagster**, que modela *assets* (datos) en lugar de tareas.

En local se ejecuta con el **docker-compose oficial** (usar la versión estable actual y seguir su documentación). No uses el servicio gestionado (MWAA) por su coste.

### 3.9 PyIceberg y DuckDB (herramientas Python)
- **PyIceberg**: librería Python para crear/leer/escribir tablas Iceberg sin Spark. Soporta catálogos SQL, REST y Glue.
- **DuckDB**: motor SQL embebido, útil para consultar Parquet/Iceberg localmente y para pruebas rápidas.

### 3.10 Calidad de datos, gobierno y regulación (conceptos del dominio)
- **Data contract**: acuerdo explícito del esquema y las reglas que deben cumplir los datos entrantes. Lo que no cumple se **rechaza** (no se arregla en silencio).
- **Calidad de señal**: SNR en dB, impedancia de electrodos en kΩ, completitud de canales y de metadatos.
- **Lineage (trazabilidad)**: poder responder de dónde viene cada dato y qué transformaciones sufrió (run_id, dag_id, hash del archivo origen).
- **ALCOA+** (integridad de datos): Attributable, Legible, Contemporaneous, Original, Accurate + Complete, Consistent, Enduring, Available. En el proyecto se traduce en: columnas de auditoría, raw inmutable, hash del archivo, timestamps de ingesta, versionado con snapshots.
- **21 CFR Part 11** (FDA): requisitos sobre registros electrónicos (audit trails, control de acceso, validación, firmas electrónicas). Solo se **menciona y se mapea conceptualmente**; no se implementa de forma completa.
- **Pseudonimización**: los identificadores de sujeto son códigos, no datos personales.

---

## 4. Modelo de datos

### 4.1 Archivo de entrada (raw)
Por cada sesión llega:
- `session_<id>.parquet` (o `.npz`): señal multicanal sintética (matriz tiempo × canales).
- `session_<id>.json`: metadatos de la sesión.

Ejemplo de metadatos:
```json
{
  "session_id": "S-000123",
  "subject_code": "SUBJ-0042",
  "study_id": "STUDY-A",
  "phase": "acute",
  "recorded_at": "2026-11-18T09:30:00Z",
  "device_id": "DEV-07",
  "sampling_rate_hz": 1000,
  "n_channels": 32,
  "duration_s": 120,
  "firmware_version": null
}
```
(`firmware_version` empieza como `null`/ausente para poder demostrar **schema evolution** más tarde.)

### 4.2 Tablas Iceberg
**`standardised.sessions`**
| Columna | Tipo | Nota |
|---|---|---|
| session_id | string | clave |
| subject_code | string | pseudónimo |
| study_id | string | |
| phase | string | acute / semi_chronic / chronic |
| recorded_at | timestamp | partición `days(recorded_at)` |
| device_id | string | |
| sampling_rate_hz | int | |
| n_channels | int | |
| duration_s | double | |
| raw_uri | string | ruta al archivo en S3 |
| source_sha256 | string | hash del archivo origen (integridad) |
| ingested_at | timestamp | auditoría |
| run_id | string | Airflow run (lineage) |

**`standardised.channel_metrics`**
| Columna | Tipo | Nota |
|---|---|---|
| session_id | string | |
| channel | int | |
| impedance_kohm | double | |
| snr_db | double | |
| rms | double | |
| quality_flag | string | ok / warning / fail |

**`quarantine.rejected_files`**
| Columna | Tipo | Nota |
|---|---|---|
| raw_uri | string | |
| check_name | string | qué validación falló |
| reason | string | descripción legible |
| owner | string | a quién se notifica |
| rejected_at | timestamp | |
| run_id | string | |

**`curated.session_quality_summary`** (ejemplo de dataset curado)
- Una fila por sesión con: nº de canales válidos, SNR medio, % de canales fallidos, `is_ml_ready` (booleano según reglas).

### 4.3 Data contract (ejemplos de reglas)
> Los umbrales son **ilustrativos e inventados**, definidos en un archivo de configuración (`contracts/session_contract.yaml`), no en el código.

- **Esquema/formato**: todos los campos obligatorios presentes y con el tipo correcto; `phase` ∈ {acute, semi_chronic, chronic}.
- **Completitud**: `n_channels` coincide con el nº de columnas de la señal; sin NaN por encima de un porcentaje.
- **Consistencia**: `duration_s ≈ n_muestras / sampling_rate_hz`.
- **Rangos**: `sampling_rate_hz` dentro de un conjunto permitido; `recorded_at` no en el futuro.
- **Calidad de señal por canal**: impedancia dentro de un rango configurable; SNR por encima de un mínimo. Un canal malo → `warning`/`fail` y se registra; si demasiados canales fallan → la sesión va a cuarentena.
- **Duplicados**: `session_id` no puede existir ya en `standardised.sessions` (idempotencia).

### 4.4 Cálculo de SNR (para el generador y las métricas)
`SNR_dB = 10 · log10( P_señal / P_ruido )`, donde P es potencia (media del cuadrado). El generador crea una señal (suma de senoides) + ruido gaussiano con una SNR objetivo conocida, de modo que se pueda **comprobar** que el cálculo la recupera. Además, el generador debe poder producir **casos malos a propósito** (canal con impedancia alta, metadatos incompletos, duplicado, formato incorrecto) para probar la cuarentena.

---

## 5. Estructura del repositorio propuesta
```
neural-lakehouse/
├── README.md
├── LEARNING_LOG.md
├── .env.example
├── .gitignore
├── pyproject.toml               # dependencias (uv/poetry/pip)
├── docker-compose.yml           # MinIO + Airflow (+ Postgres para Airflow)
├── contracts/
│   └── session_contract.yaml
├── src/neural_lakehouse/
│   ├── config.py
│   ├── generator/               # datos sintéticos (señal + metadatos + casos malos)
│   ├── quality/                 # validaciones, SNR, impedancia, contrato
│   ├── lakehouse/               # catálogo, creación de tablas, escritura Iceberg
│   └── utils/                   # hashing, logging, S3 helpers
├── dags/
│   └── ingest_sessions_dag.py
├── sql/
│   ├── trino/                   # consultas de ejemplo y time travel (equivalente a Athena)
│   └── snowflake/               # setup + consultas (documentados, ejecución opcional)
├── infra/
│   └── tofu/                    # OpenTofu: S3, Glue DB, IAM (solo fmt/validate, sin desplegar)
├── tests/
│   ├── test_snr.py
│   ├── test_contract.py
│   └── test_idempotency.py
└── .github/workflows/ci.yml     # lint + tests
```

---

## 6. Plan por fases

> Cada fase tiene: objetivo, tareas, **criterio de hecho** y **preguntas de entrevista**. Claude Code debe seguir este orden y pedirme confirmación al cerrar cada fase.

### Fase 0: Preparación (1-2 horas)
**Tareas**
- Crear el repo, `pyproject.toml`, `.gitignore`, `.env.example`, estructura de carpetas.
- Instalar y comprobar Docker y Python.
- Configurar `ruff` (lint) y `pytest`.

**Hecho cuando**: `pytest` corre (aunque sea con un test trivial) y el repo está inicializado con un primer commit.

---

### Fase 1: Todo en local, gratis (1-2 días) ⭐ la más importante
**Objetivo**: entender Iceberg y Airflow sin gastar nada.

**Tareas**
1. `docker-compose.yml` con **MinIO** (crear buckets `raw` y `lakehouse` al arrancar) y **Airflow** (docker-compose oficial, versión estable).
2. **Generador de datos sintéticos**: sesiones con señal + metadatos, con SNR objetivo conocido y un porcentaje de casos malos (parametrizable, con semilla aleatoria fija para reproducibilidad).
3. **Funciones de calidad** con tests unitarios: SNR, impedancia, completitud, consistencia de duración, validación del contrato.
4. **Catálogo Iceberg local** con PyIceberg (catálogo SQL con SQLite o Postgres y *warehouse* en MinIO).
5. **Crear las tablas** `standardised.sessions`, `standardised.channel_metrics`, `quarantine.rejected_files` con particionado `days(recorded_at)`.
6. **Escritura**: append de sesiones válidas; rechazadas a cuarentena.
7. **Experimentos Iceberg** (anotar resultados en `LEARNING_LOG.md`):
   - Hacer varios *appends* y listar los **snapshots**.
   - **Time travel**: leer la tabla en un snapshot anterior.
   - **Schema evolution**: añadir la columna `firmware_version` y ver que los datos viejos siguen leyéndose (con `null`).
   - Inspeccionar los archivos reales en MinIO: ver `data/` y `metadata/` y abrir un `metadata.json`.
   - **Compactación / expiración de snapshots** (mantenimiento) y ver cómo cambian los archivos.
8. **DAG de Airflow** `ingest_sessions_dag`:
   - Detecta archivos nuevos en `raw` (sensor o listado).
   - *Dynamic task mapping*: una tarea por archivo.
   - Tareas: validar → calcular métricas → escribir (standardised o quarantine).
   - **Idempotente**: reejecutar no duplica sesiones (comprobar `session_id` antes o usar `MERGE`/upsert).
   - Reintentos configurados y callback de fallo (puede ser solo un log).
   - Probar **backfill** de un rango de fechas.
9. Tabla **curated** con un resumen por sesión.

**Hecho cuando**
- Genero 50-100 sesiones (con ~10-15 % malas), ejecuto el DAG, y veo: sesiones válidas en `standardised`, malas en `quarantine` con el motivo, y puedo hacer time travel.
- Reejecutar el DAG sobre los mismos archivos **no crea duplicados**.
- Puedo explicar con mis palabras la jerarquía catálogo → metadata.json → manifest list → manifest → data files.

**Preguntas de entrevista (practicar en inglés)**
- What problem does Iceberg solve compared to plain Parquet files on S3?
- What is a snapshot and how does time travel work?
- How would you make an Airflow task idempotent? Why does it matter?
- How would you handle a corrupted or non-conforming file arriving in the pipeline?

---

### Fase 2: "Estilo AWS", pero 100 % gratis (1-2 días)
**Objetivo**: practicar la arquitectura de AWS (S3 + Glue Catalog + Athena + Glue Jobs) con **equivalentes de código abierto que corren en tu ordenador**, y aprender el mapeo uno a uno para poder hablar de AWS en la entrevista sin tener cuenta.

**Equivalencias que vamos a usar**
| AWS | Equivalente gratuito y local | Qué aprendes |
|---|---|---|
| S3 | MinIO (ya usado en Fase 1) | Buckets, prefijos, API S3 |
| Glue Data Catalog | Catálogo **REST de Iceberg** open source (p. ej. Apache Polaris o Lakekeeper) | Qué es un catálogo y cómo lo comparten varios motores |
| Athena | **Trino** (conector Iceberg) y/o DuckDB | SQL sobre tablas Iceberg, time travel en SQL, `MERGE` |
| Glue Job (Spark) | **PySpark local** con el runtime de Iceberg | Transformaciones Spark sobre Iceberg |
| IAM | Políticas IAM de ejemplo escritas en JSON (documentadas, no desplegadas) | Mínimo privilegio |
| CloudFormation / Terraform | **OpenTofu** (gratuito), solo `fmt` y `validate` | Infraestructura como código |

> Comprueba la disponibilidad y la licencia actuales de cada imagen/herramienta antes de usarla (algunas, como MinIO, han cambiado su política de distribución; si no está disponible, usa una alternativa compatible con S3 como SeaweedFS o Garage).

**Tareas**
1. **Catálogo REST de Iceberg** en Docker. Cambiar la configuración de PyIceberg para usarlo. **El código debe aislar la configuración del catálogo**, de modo que pasar a Glue (`type: glue`) en el futuro sea solo cambiar configuración. Documenta esto en el README.
2. **Trino en Docker** con el conector Iceberg apuntando al mismo catálogo y a MinIO. Con Trino:
   - Consultar `standardised.sessions` y `channel_metrics`.
   - **Time travel en SQL** (`FOR VERSION AS OF` / `FOR TIMESTAMP AS OF`, comprobar sintaxis actual).
   - Hacer un `MERGE` y ver el nuevo snapshot.
   - Mantenimiento: `optimize` (compactación) y `expire_snapshots`, comprobando el efecto en los archivos de MinIO.
3. **Comparar motores**: mismas consultas desde PyIceberg, DuckDB y Trino; mismos resultados. Anotar en `LEARNING_LOG.md` qué hace cada uno mejor.
4. **(Opcional) PySpark local** con el runtime de Iceberg: leer una tabla y escribir un agregado en `curated`. Esto emula un Glue Job; entender qué es un DAG de Spark, una partición y un shuffle.
5. **IaC con OpenTofu (sin desplegar nada)**: escribir en `infra/tofu/` la definición de lo que sería el entorno en AWS (bucket S3 con cifrado y bloqueo de acceso público, base de datos de Glue, rol IAM de mínimo privilegio). Ejecutar solo `tofu fmt` y `tofu validate`. **No hacer `apply`**: no hay cuenta de AWS y no hace falta.
6. **Escribir una política IAM de mínimo privilegio** (JSON de ejemplo en `docs/`) para el pipeline y para un lector de solo lectura; explicar cada permiso.

**Hecho cuando**
- Consulto las mismas tablas Iceberg desde PyIceberg, DuckDB y Trino, y hago time travel en SQL.
- Puedo dibujar de memoria cómo se relacionan S3, Iceberg, el catálogo y un motor SQL, y decir qué pieza de mi proyecto equivale a cada servicio de AWS.
- `tofu validate` pasa sin errores.

**Nota de honestidad (para el README y la entrevista)**: esta fase **no usa servicios reales de AWS**. Practica los mismos conceptos con herramientas equivalentes. Lo que no se practica es la configuración específica de AWS (consola, permisos reales, facturación).

**Preguntas de entrevista**
- How do S3, Iceberg and the Glue Data Catalog work together when Athena runs a query?
- What would change in your project if you moved it from MinIO + REST catalog to S3 + Glue?
- Why use infrastructure as code instead of creating resources in the console?
- How would you apply least-privilege IAM to this pipeline?
- What are the cost drivers in Athena and Glue, and how would you control them?

---

### Fase 3: Snowflake y el warehouse (opcional, prueba gratuita de 30 días)
**Objetivo**: entender qué es Snowflake y cómo se trabaja con él, usando solo lo que se puede hacer gratis.

**Limitación importante (sin cuenta de AWS)**: leer tablas Iceberg de S3 desde Snowflake (external volume + catalog integration) **requiere un almacenamiento en la nube propio**, así que **no se puede practicar gratis**. Se estudia como **concepto** (sección 3.7) y se practica lo demás.

**Parte A: sin cuenta, solo conceptos y SQL (obligatoria)**
- Estudiar: virtual warehouses, separación almacenamiento/cómputo, roles, stages, `COPY INTO`, Time Travel y Zero-Copy Clone nativos de Snowflake, y la diferencia entre tablas Iceberg gestionadas por Snowflake y externas.
- Practicar la **parte analítica de SQL** (CTEs, funciones de ventana, vistas, agregaciones) sobre las tablas con DuckDB/Trino. El SQL es muy parecido al de Snowflake.
- Escribir en `sql/snowflake/` los scripts que ejecutarías (setup y consultas), **comentados y marcados como "no ejecutados"**, y verificar su sintaxis en la documentación oficial.

**Parte B: con la prueba gratuita (opcional, hacerla cerca de la entrevista)**
- Snowflake ofrece una **prueba gratuita de 30 días**. El reloj corre desde el registro, así que **actívala solo cuando vayas a usarla de verdad**. Comprueba las condiciones actuales: si te pide tarjeta de crédito, no te registres.
- Crear un warehouse **XS** con `AUTO_SUSPEND` bajo.
- Subir los Parquet exportados de `curated` a un **stage interno**, cargarlos en una tabla nativa con `COPY INTO` y hacer consultas.
- Probar **Time Travel** nativo y **clonado** de una tabla.
- Anotar diferencias con Trino/Athena en `LEARNING_LOG.md`.

**Hecho cuando**: sé explicar qué es Snowflake, cómo se cobra, cómo se relaciona con un lakehouse en S3 y qué haría para exponer mis tablas Iceberg en él (aunque no lo haya podido ejecutar).

**Preguntas de entrevista**
- Why would you expose Iceberg tables in Snowflake instead of loading the data into it?
- What are the trade-offs between Snowflake-managed and externally managed Iceberg tables?
- How does compute/storage separation work in Snowflake and how does it affect cost?

---

### Fase 4: Calidad, observabilidad y CI/CD (1 día)
**Tareas**
1. **Observabilidad de datos** básica: métricas por ejecución (nº archivos procesados/rechazados, SNR medio, tiempo) guardadas en una tabla `ops.pipeline_runs`; detectar **schema drift** (columnas nuevas o tipos distintos en los metadatos entrantes).
2. **Alertas**: callback de fallo de Airflow (log, o email/Slack simulado).
3. **Tests**: unitarios (SNR, contrato), de integración sobre MinIO, y test de idempotencia.
4. **CI con GitHub Actions**: lint (`ruff`), tests (`pytest`) en cada push. (Si hay OpenTofu: `tofu fmt` y `tofu validate`.)
5. **Gobierno y trazabilidad**: documentar cómo el proyecto cubre ALCOA+ (tabla de mapeo en el README) y qué faltaría para un entorno regulado real (validación formal, e-signatures, control de accesos, etc.).
6. **README final** con diagrama, decisiones de diseño, qué haría distinto en producción y cómo ejecutar el proyecto desde cero.

**Hecho cuando**: CI en verde, README claro, y puedo presentar el proyecto en 5-10 minutos.

---

## 7. Mapa ALCOA+ del proyecto (para el README y la entrevista)
| Principio | Cómo se refleja |
|---|---|
| Attributable | `run_id`, `dag_id`, usuario/servicio de ingesta en columnas de auditoría |
| Legible | Esquema documentado, contrato en YAML, README |
| Contemporaneous | `ingested_at` y `recorded_at` registrados en el momento |
| Original | Zona raw inmutable + `source_sha256` |
| Accurate | Validaciones de rango/consistencia; SNR verificado con datos de SNR conocida |
| Complete | Checks de completitud; nada se descarta en silencio (cuarentena con motivo) |
| Consistent | Esquema común en standardised; ACID de Iceberg |
| Enduring | Snapshots de Iceberg, S3 con versioning |
| Available | Consultable por Athena/Snowflake/PyIceberg con catálogo central |

---

## 8. Comparativas útiles (para estudiar)

**Data lake vs data warehouse vs lakehouse**
| | Data lake | Data warehouse | Lakehouse |
|---|---|---|---|
| Almacén | Archivos en object storage | Almacenamiento propio del motor | Object storage + formato de tabla |
| Estructura | Poca, "schema on read" | Fuerte, "schema on write" | Tablas con esquema y transacciones |
| Transacciones | No | Sí | Sí (Iceberg/Delta/Hudi) |
| Coste almacenamiento | Bajo | Mayor | Bajo |

**Athena vs Spark (Glue) vs Snowflake**
| | Athena | Spark (Glue/EMR) | Snowflake |
|---|---|---|---|
| Uso típico | SQL ad hoc y exploración | Transformaciones pesadas, ML | Analítica SQL a escala, BI |
| Infraestructura | Serverless | Clúster/jobs | Warehouses gestionados |
| Cobro | Por datos escaneados | Por tiempo de cómputo | Por tiempo de warehouse + almacenamiento |

**Airflow vs Dagster**
| | Airflow | Dagster |
|---|---|---|
| Modelo | Tareas y DAGs | Assets (datos) y su linaje |
| Madurez | Muy extendido | Más moderno |
| Fuerte en | Orquestación general | Linaje, tests y desarrollo local |

---

## 9. Glosario rápido
- **ACID**: Atomicity, Consistency, Isolation, Durability (garantías transaccionales).
- **Backfill**: reprocesar intervalos pasados.
- **Catálogo**: servicio que dice qué tablas existen y dónde está su metadato actual.
- **Compaction**: unir archivos pequeños en otros más grandes.
- **Data contract**: acuerdo de esquema y calidad entre productor y consumidor de datos.
- **DAG**: grafo acíclico dirigido de tareas.
- **Idempotencia**: ejecutar varias veces produce el mismo resultado que una.
- **Lineage**: trazabilidad del origen y transformaciones de un dato.
- **Manifest**: archivo de Iceberg con la lista de data files y sus estadísticas.
- **Partition pruning**: saltar particiones/archivos que no hacen falta según el filtro.
- **Schema drift**: cambio inesperado del esquema de los datos entrantes.
- **Snapshot**: versión de una tabla Iceberg en un commit.
- **SNR**: relación señal-ruido, en dB.
- **Time travel**: consultar datos tal como estaban en un snapshot o instante anterior.

---

## 10. Cómo presentar el proyecto en la entrevista

**Pitch de 30 segundos (inglés)**
> "To prepare for this kind of role, I built a small lakehouse that simulates neural recording sessions. Files land in S3-compatible storage, an Airflow DAG validates them against a data contract, checking signal-to-noise ratio, impedance and metadata completeness, and writes valid sessions to Iceberg tables, with rejected files going to a quarantine table with the reason. I queried the same tables from several engines and used time travel to see previous versions. I ran everything locally with open-source equivalents of the AWS services, so I know the architecture and how it maps to S3, Glue and Athena, even though I haven't run it on a real AWS account. I used synthetic data, so it's a learning project, not a production system."

**Frases honestas para los huecos**
- "I haven't run Iceberg in production, but I've built a project end to end, including schema evolution, time travel and maintenance."
- "I used open-source equivalents locally (MinIO, an Iceberg REST catalog, Trino) instead of a real AWS account, so I understand the mapping to S3, Glue and Athena, but I'd need to learn the AWS-specific configuration on the job."
- "I wrote Terraform-style infrastructure code and validated it, but I haven't deployed it to a real cloud account."
- "Airflow was run locally with Docker; I understand scheduling, retries, idempotency and backfills."
- "I haven't used Snowflake in production; I studied its architecture and practiced the SQL side."

**Qué decir si preguntan "¿qué harías distinto en producción?"**
- Orquestación gestionada y alta disponibilidad; secretos en un gestor (AWS Secrets Manager).
- Control de accesos más fino, cifrado con KMS y auditoría (CloudTrail).
- Validación formal del sistema y firmas electrónicas si es entorno regulado (Part 11).
- Observabilidad completa (métricas, alertas, linaje automático), compactación programada y política de retención.
- Tests de rendimiento con volúmenes reales y estrategia de particionado revisada.

---

## 11. Orden recomendado si hay poco tiempo
1. **Fase 0 + Fase 1** (lo esencial: Iceberg + Airflow + calidad, todo en local y gratis).
2. **Fase 2** (equivalentes de AWS: catálogo REST + Trino) para entender el mapeo con S3, Glue y Athena.
3. **Fase 4** (tests, CI, README), aunque sea parcialmente, para poder presentarlo.
4. **Fase 3** (Snowflake): primero la Parte A (conceptos y SQL). La prueba de 30 días solo si queda tiempo y la entrevista está cerca.

Si solo hay tiempo para la Fase 1 bien hecha, ya es un proyecto defendible en la entrevista.

---

## 12. Primer mensaje sugerido para Claude Code
> "Lee `PROYECTO_lakehouse_neural_quality.md` completo. Todo el proyecto debe usar solo herramientas gratuitas y de código abierto, sin cuenta de AWS. Empieza por la Fase 0. Antes de escribir nada, resúmeme en 10 líneas cómo vas a trabajar conmigo según la sección 0 y qué vamos a construir en la Fase 1. Después prepara la Fase 0 y pídeme confirmación antes de pasar a la Fase 1."

---

## 13. Mapa de herramientas: AWS, versión gratuita y Microsoft/Azure

La empresa objetivo usa **AWS y herramientas de Microsoft** (no se conoce el detalle exacto). Esta tabla sirve para entender los equivalentes y no perderse si en la entrevista se menciona un servicio de otra nube. **No hace falta práctica con Azure**: basta con conocer el concepto y su equivalente.

| Función | AWS | Versión gratuita del proyecto | Microsoft / Azure |
|---|---|---|---|
| Almacenamiento de objetos | S3 | MinIO | Azure Blob Storage / ADLS Gen2 |
| Catálogo de datos | Glue Data Catalog | Catálogo REST de Iceberg | Microsoft Purview, catálogo de Fabric |
| SQL sobre el lake | Athena | Trino / DuckDB | Synapse serverless SQL, Fabric SQL endpoint |
| Transformaciones Spark | Glue Jobs, EMR | PySpark local | Azure Databricks, Fabric Spark, Synapse Spark |
| Orquestación | MWAA (Airflow gestionado), Step Functions | Airflow en Docker | Azure Data Factory, Airflow en Fabric |
| Warehouse | Redshift / Snowflake | DuckDB / Trino (+ Snowflake en prueba) | Fabric Warehouse, Synapse |
| Identidad y permisos | IAM | Políticas de ejemplo en JSON | Microsoft Entra ID + Azure RBAC |
| Secretos | Secrets Manager | `.env` local | Azure Key Vault |
| Contenedores | ECS / EKS | Docker Compose | Container Apps / AKS |
| Funciones serverless | Lambda | n/a | Azure Functions |
| Monitorización | CloudWatch | Logs de Airflow | Azure Monitor |
| IaC | CloudFormation, Terraform | OpenTofu | Bicep / ARM, Terraform |
| CI/CD | CodePipeline, GitHub Actions | GitHub Actions | Azure DevOps Pipelines, GitHub Actions |
| IA / LLM | Bedrock, SageMaker | n/a | Azure OpenAI / AI Foundry, Azure Machine Learning |
| BI y colaboración | QuickSight | n/a | Power BI, Teams, SharePoint, Microsoft 365 |

**Cómo aprender Microsoft gratis**: Microsoft Learn (cursos gratuitos con rutas de Azure, Fabric y Power BI) y Power BI Desktop (gratuito). Las cuentas de Azure con crédito suelen pedir tarjeta, así que no son necesarias para este plan.

**Pregunta útil para la entrevista**: *"Which Microsoft tools does the data team use day to day, for example Azure services, Power BI or Teams?"*

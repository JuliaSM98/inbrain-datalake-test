# Ideas clave (chuleta para la entrevista)

> Documento vivo: se reescribe cada vez que aprendemos algo nuevo. Si solo puedes leer una cosa
> antes de la entrevista, lee esto. Cada idea marcada con ✅ ya la has visto funcionar en el proyecto.

---

## 🧊 Apache Iceberg (formato de tabla, open source, NO es de AWS)
1. Parquet es un **archivo**; Iceberg es una **tabla**: una capa de metadatos que dice qué
   archivos Parquet forman la tabla en cada momento.
2. Cada escritura crea un **snapshot** nuevo sin modificar nada anterior → de ahí salen
   **time travel** y **ACID** (los lectores siempre ven un snapshot completo y consistente).
3. ✅ Jerarquía: **catálogo → metadata.json → manifest list → manifests → data files**.
   El catálogo solo guarda un **puntero** al `metadata.json` actual.
4. ✅ **Un commit = escribir archivos nuevos + mover el puntero del catálogo de forma atómica**
   (compare-and-swap). Si otro escribió antes, el commit falla y se reintenta (*optimistic concurrency*).
   Nada se modifica: todo se añade (encaja con S3 inmutable).
5. ✅ **Un snapshot es una foto completa, no un diff**: su manifest list reutiliza manifests anteriores.
   Time travel = empezar la lectura desde la manifest list de otro snapshot. No se copia nada.
6. ✅ Los **manifests guardan min/max por columna y archivo** → el motor salta archivos sin abrirlos
   (*file pruning*) y **sin listar S3**.
7. ✅ Las tablas son **archivos autodescriptivos**: si pierdes el catálogo, no pierdes datos
   (`register_table` con el último metadata.json). Pero el catálogo es la *source of truth* del
   metadata.json vigente → necesita backup.
8. ✅ Coste: muchos commits pequeños → muchos archivos pequeños (*small files problem*) → **compaction**.
9. ✅ **Delete = copy-on-write**: se reescribe cada archivo con alguna fila afectada (los viejos se quedan
   para time travel). Si el delete coincide con la partición, solo se quitan archivos de la lista.
   Alternativa: **merge-on-read** (delete files pequeños aplicados al leer; Spark/Trino).
10. ✅ **Time travel** por snapshot id o por timestamp. **Tag** = nombre fijo para un snapshot
    (datasets ML reproducibles) y lo **protege** de expiración. **Rollback** mueve "current" atrás
    **sin borrar** el snapshot malo → audit trail intacto.
11. ✅ **Expire snapshots** = quitar snapshots viejos de los metadatos → ya **no hay time travel** a ellos.
    Los archivos que nadie referencia se borran (Spark/Trino) o quedan como **orphans** (PyIceberg).
    La retención es decisión de **negocio/compliance**, no solo técnica.
12. ✅ **Parquet = formato de archivo; data file = rol** (lo referencia un manifest). Los motores
    nunca listan carpetas: un Parquet no referenciado no existe para la tabla.
13. ✅ **Schema evolution por field_id** (guardado también *dentro* de cada Parquet):
    add → `null` en archivos viejos · rename → solo metadatos · drop + re-add mismo nombre → ID nuevo,
    **sin datos zombis** · cambios de tipo solo seguros (int→long, float→double).
    Parquet suelto / Hive emparejan **por nombre** → renames rotos y zombis.
14. ✅ Cambiar el esquema **no crea snapshot** ni reescribe datos: solo un metadata.json con nuevo schema-id.
15. ✅ Drop column **no borra físicamente** los datos → borrado real (GDPR) = reescribir + expirar.
16. ✅ **Hidden partitioning**: partición = *transform* de una columna real (`days(recorded_at)`,
    `months`, `hours`, `bucket(N, id)`, `truncate`). Iceberg calcula el valor al escribir y lo guarda
    en los manifests; el usuario filtra por `recorded_at` y obtiene pruning. Sin columna artificial
    → nadie puede calcularla mal. Las carpetas `x=...` son solo cosméticas: Iceberg no las usa.
17. ✅ **Partición = coste, nunca resultado**: decide qué archivos se abren; el filtro se aplica a los
    valores reales. Transforms de tiempo en **UTC**.
18. ✅ **Partition evolution**: cada archivo recuerda su `spec_id`; cambiar day→month no reescribe nada
    y las consultas planifican con todos los specs a la vez.
19. ✅ **Delete alineado con la partición = metadata-only** (no reescribe). Particionar bien abarata
    lecturas **y** borrados (p. ej. GDPR).
20. ✅ **Over-partitioning**: muchas particiones pequeñas → *small files*. Particionar solo cuando cada
    partición tiene cientos de MB; si no, sin partición / `months` / `bucket` / sort order + min/max.
21. ✅ **ACID por tabla, no entre tablas**: con varias tablas, el **marcador al final** (la tabla que
    lee la unicidad) y lo anterior **idempotente** (`overwrite` con filtro = delete+insert en 1 commit).
22. ✅ **Partición ≠ nº de archivos**: cada commit escribe archivos nuevos; 5 appends diarios = 5
    archivos en la misma partición mensual → **compactación** programada los junta.
23. ✅ Cada ejecución = un snapshot → "¿cómo estaba la tabla tras el día X?" es un time travel.

## 🗂️ AWS Glue Data Catalog (el catálogo)
1. Es el **catálogo**: sabe qué tablas existen y dónde está su `metadata.json` actual.
   **No guarda datos.**
2. Es lo que permite que **Athena, Spark y Snowflake vean la misma tabla**.
3. "Glue" son dos cosas: **Catalog** (metadatos) y **Jobs** (cómputo Spark, de pago por DPU-hora).
4. Glue ofrece también un **endpoint Iceberg REST** → nuestro REST catalog local "habla el mismo idioma".

## ❄️ Snowflake (data warehouse)
1. **Almacenamiento y cómputo separados**; pagas por el tiempo que los *virtual warehouses*
   están encendidos (de ahí `AUTO_SUSPEND`).
2. Puede **leer tablas Iceberg de tu S3** sin copiarlas (externa, solo lectura, hay que refrescar)
   o **gestionarlas él** (Snowflake-managed).
3. Tiene su propio **Time Travel** y **Zero-Copy Clone** (conceptos parecidos a los de Iceberg).

## ✅ Calidad de datos y data contracts
1. **Data contract** = acuerdo escrito y **versionado** productor↔consumidor: esquema, reglas,
   qué pasa si no se cumple y **a quién avisar** (owner). Reglas en YAML, no en código.
2. **Lo que no cumple se rechaza (cuarentena con motivo), nunca se arregla en silencio.**
3. Una función por regla; **se reportan todos los problemas a la vez**.
4. Campo desconocido → **warn** (schema drift registrado), no reject.
5. **Duplicado ≠ reprocesado**: unicidad por `session_id` **+ archivo de origen**
   (`raw_uri` = dónde está, `sha256` = qué contiene). Mismo archivo → no-op.
   Misma ruta con otra huella → raw modificado → alarma (ALCOA+ *Original*).
6. Errores transitorios (red) **no** van a cuarentena: que falle la tarea y se reintente.
7. Cada registro guarda la **versión del contrato** con la que se validó (trazabilidad).

## 🌀 Apache Airflow (orquestador)
1. **Idempotencia** (preparada en 2b): reintentos y backfills ejecutan lo mismo varias veces →
   cada paso debe poder ejecutarse 2 veces sin cambiar nada la segunda (`ensure_column`).
   Migraciones de esquema: en **una sola tarea previa**, no en tareas paralelas por archivo.
2. (se completa en el paso 7)

## 🪣 S3 / MinIO (almacenamiento de objetos)
1. Objetos inmutables en buckets con keys; no hay carpetas reales, solo prefijos.
   Para "cambiar" un archivo se escribe otro nuevo → por eso hace falta Iceberg encima.
2. "S3" es también un **protocolo/API**: MinIO, RustFS, SeaweedFS, Garage, Ceph... lo implementan.
   Por eso el mismo código vale en local y en AWS cambiando el endpoint.
3. **Riesgo de proveedor**: MinIO dejó de distribuir imágenes (2025) → los composes con
   `minio:latest` se rompen. Fijar versiones **y** tener alternativa (el protocolo S3 lo permite).

## 🧰 Buenas prácticas transversales
1. **Reproducibilidad**: versiones fijadas (`uv.lock`, tags de imágenes Docker, semillas aleatorias).
2. **Secretos** fuera del código y de Git (`.env` local ↔ Secrets Manager en AWS).
3. ✅ **Nunca esconder errores** (`except:` desnudo): un pipeline que oculta fallos no es auditable.
   Captura solo lo que sabes manejar; lo demás, que suba.
4. ✅ **Timestamps siempre en UTC con zona explícita** (ALCOA+ *Contemporaneous*).
5. ✅ **Configuración del catálogo aislada** en un solo sitio (`get_catalog()`): SQLite → REST → Glue
   sin tocar el pipeline.

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

## 🌀 Apache Airflow (orquestador)
1. (se completa en el paso 7)

## 🪣 S3 / MinIO (almacenamiento de objetos)
1. Objetos inmutables en buckets con keys; no hay carpetas reales, solo prefijos.
   Para "cambiar" un archivo se escribe otro nuevo → por eso hace falta Iceberg encima.

## 🧰 Buenas prácticas transversales
1. **Reproducibilidad**: versiones fijadas (`uv.lock`, tags de imágenes Docker, semillas aleatorias).
2. **Secretos** fuera del código y de Git (`.env` local ↔ Secrets Manager en AWS).
3. ✅ **Nunca esconder errores** (`except:` desnudo): un pipeline que oculta fallos no es auditable.
   Captura solo lo que sabes manejar; lo demás, que suba.
4. ✅ **Timestamps siempre en UTC con zona explícita** (ALCOA+ *Contemporaneous*).
5. ✅ **Configuración del catálogo aislada** en un solo sitio (`get_catalog()`): SQLite → REST → Glue
   sin tocar el pipeline.

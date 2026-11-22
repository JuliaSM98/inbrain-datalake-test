# Ideas clave (chuleta para la entrevista)

> Documento vivo: se reescribe cada vez que aprendemos algo nuevo. Si solo puedes leer una cosa
> antes de la entrevista, lee esto. Cada idea marcada con ✅ ya la has visto funcionar en el proyecto.

---

## 🧊 Apache Iceberg (formato de tabla, open source, NO es de AWS)
1. Parquet es un **archivo**; Iceberg es una **tabla**: una capa de metadatos que dice qué
   archivos Parquet forman la tabla en cada momento.
2. Cada escritura crea un **snapshot** nuevo sin modificar nada anterior → de ahí salen
   **time travel** y **ACID** (los lectores siempre ven un snapshot completo y consistente).
3. Jerarquía: **catálogo → metadata.json → manifest list → manifests → data files**.
   El catálogo solo guarda un **puntero** al `metadata.json` actual.

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

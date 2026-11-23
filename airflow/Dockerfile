# Our Airflow image = the official Airflow 3.4.0 image + the libraries our tasks need.
#
# Why: the tasks call neural_lakehouse.pipeline.ingest(), which imports pyiceberg, pyyaml...
# Our own code is NOT copied here: it is mounted from the repo (see docker-compose.yml),
# so editing a .py file on your Mac is seen by Airflow immediately.
FROM apache/airflow:3.4.0

# "apache-airflow==${AIRFLOW_VERSION}" pins Airflow itself, so pip can't silently change it
# while installing our libraries (if there is a conflict, the build fails loudly instead).
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "pyiceberg[pyarrow,sql-sqlite,pyiceberg-core]==0.12.0" \
    "pyyaml==6.0.4" \
    "python-dotenv==1.2.4" \
    "numpy==2.5.5"

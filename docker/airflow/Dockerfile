FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Python deps for Airflow DAGs (producer, kafka, delta, etc.)
RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    delta-spark==3.1.0 \
    pyspark==3.5.1 \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0
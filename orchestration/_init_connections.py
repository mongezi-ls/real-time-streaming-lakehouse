"""
Idempotently create Airflow connections + variables used by DAGs.
Run this inside the Airflow container once after startup.
"""
import os
from airflow import settings
from airflow.models import Connection, Variable


def upsert_connection(conn_id, conn_type, **kwargs):
    session = settings.Session()
    existing = session.query(Connection).filter(Connection.conn_id == conn_id).one_or_none()
    if existing:
        for k, v in kwargs.items():
            setattr(existing, k, v)
        session.merge(existing)
    else:
        session.add(Connection(conn_id=conn_id, conn_type=conn_type, **kwargs))
    session.commit()
    session.close()
    print(f"Connection '{conn_id}' upserted")


def upsert_variable(key, value):
    Variable.set(key, value)
    print(f"Variable '{key}' set")


if __name__ == "__main__":
    token = os.environ.get("DBT_DATABRICKS_TOKEN", "")
    if not token:
        raise SystemExit("DBT_DATABRICKS_TOKEN not set")

    upsert_connection(
        conn_id="databricks_default",
        conn_type="http",
        host="dbc-d5cf33b9-b470.cloud.databricks.com",
        schema="https",
        login="token",
        password=token,
        port=443,
    )

    upsert_variable("databricks_http_path", "/sql/1.0/warehouses/845db861b14f5164")
    upsert_variable("databricks_catalog",   "workspace")
    upsert_variable("databricks_schema",    "default_marts")

    print("Done.")
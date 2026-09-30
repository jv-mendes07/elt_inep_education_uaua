"""Ensures Superset's own metadata database ("superset") exists before
`superset db upgrade` runs — mounted into the container and invoked from
the startup command in docker-compose.yml.

Superset's own dashboards/charts/datasets/users are metadata distinct from
the project's warehouse (raw/staging/marts, in the "inep" database).
Mirrors the same fix already applied to Airflow: a dedicated metadata
database per tool, never the warehouse, created idempotently so a
container restart never wipes anything.
"""
import os

import psycopg2

conn = psycopg2.connect(
    host="postgres",
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname="postgres",
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'superset'")
    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE superset")
        print("Created database 'superset'.")
    else:
        print("Database 'superset' already exists.")
conn.close()

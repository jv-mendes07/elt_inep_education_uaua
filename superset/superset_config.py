"""Local/dev Superset config, mounted into the superset container at
/app/pythonpath/superset_config.py (that directory is on the official
image's PYTHONPATH, so Superset auto-imports this module on startup).
"""
import os

# The base apache/superset image's own default config never reads the
# DATABASE_URL env var (that mapping only exists in Superset's own example
# docker-compose setup, which this project doesn't use) — so despite
# DATABASE_URL being set in docker-compose.yml, Superset was silently
# falling back to SQLite at /app/superset_home/superset.db, a path with no
# volume behind it. Every dashboard/chart/dataset you save would be gone
# the next time this container is recreated. Wiring it explicitly here
# points Superset's own metadata at Postgres instead (a dedicated
# "superset" database — see ensure_superset_db.py — not the "inep"
# warehouse, mirroring the same fix already applied to Airflow).
SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]

# PREVENT_UNSAFE_DB_CONNECTIONS defaults to True — Superset's built-in SSRF
# guard, which rejects any database connection whose hostname resolves to a
# private/internal IP address. That's exactly what "postgres" (the sibling
# container on the docker-compose network, e.g. 172.18.0.x) resolves to, so
# the default blocks the one connection this project actually needs, with
# the UI just saying "The hostname provided can't be resolved."
#
# Safe to disable here: this is a single-user local dev/analytics stack,
# not a multi-tenant deployment where an untrusted user could otherwise
# abuse Superset to probe the internal network — the concern this flag
# guards against doesn't apply.
PREVENT_UNSAFE_DB_CONNECTIONS = False

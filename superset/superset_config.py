"""Superset configuration for the local demo (mounted into the Superset container)."""

import os

# Signs session cookies and encrypts stored database passwords.
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Superset's own metadata (users, charts, dashboards) lives in a separate
# `superset` database on the shared PostgreSQL instance.
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_METADATA_URI"]

# Dashboard queries aggregate ~6M rows; cache results for 5 minutes in memory.
# Good enough for one local process - production would use Redis.
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

"""PostgreSQL connections to the warehouse."""

from __future__ import annotations

import psycopg

from src.config import settings as config

_SESSION_OPTIONS = (
    f"-c statement_timeout={config.DB_STATEMENT_TIMEOUT} -c lock_timeout={config.DB_LOCK_TIMEOUT}"
)


def connect(dsn: str) -> psycopg.Connection:
    """Open a warehouse connection with the pipeline's safety timeouts applied.

    Used as ``with connect(dsn) as conn``: commits on success, rolls back on error.
    """
    return psycopg.connect(
        dsn,
        connect_timeout=config.DB_CONNECT_TIMEOUT_S,
        options=_SESSION_OPTIONS,
        application_name=config.APPLICATION_NAME,
    )

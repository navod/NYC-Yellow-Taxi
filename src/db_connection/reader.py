"""Run the project's SQL files (``sql/``) against a connection."""

from __future__ import annotations

from typing import Any

import psycopg

from src.config import settings as config


def read_sql(relative_path: str) -> str:
    """Read a SQL file from the project's ``sql/`` directory."""
    return (config.SQL_DIR / relative_path).read_text()


def run_sql_file(
    conn: psycopg.Connection, relative_path: str, params: dict[str, Any] | None = None
) -> int:
    """Execute a SQL file and return the affected row count."""
    cursor = conn.execute(read_sql(relative_path), params)
    return cursor.rowcount

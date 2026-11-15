"""Load: bulk-copy cleaned data into staging, then build the star schema with SQL."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import psycopg
import pyarrow.parquet as pq
from psycopg import sql

from etl import config
from etl.utils.logging_utils import get_logger
from schema import INTEGER_COLUMNS, STAGING_COLUMNS

logger = get_logger(__name__)

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


def read_sql(relative_path: str) -> str:
    """Read a SQL file from the project's ``sql/`` directory."""
    return (config.SQL_DIR / relative_path).read_text()


def run_sql_file(
    conn: psycopg.Connection, relative_path: str, params: dict[str, Any] | None = None
) -> int:
    """Execute a SQL file and return the affected row count."""
    cursor = conn.execute(read_sql(relative_path), params)
    return cursor.rowcount


def create_schema(dsn: str) -> None:
    """Create tables (if missing) and seed the static dimensions."""
    with connect(dsn) as conn:
        run_sql_file(conn, "schema.sql")
        run_sql_file(conn, "seed_dimensions.sql")

def load_dimensions(dsn: str, zone_csv: Path, month: str) -> dict[str, int]:
    """Refresh the location dimension and extend the date dimension for ``month``."""
    month_start, month_end = config.month_bounds(month)

    with connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("TRUNCATE staging.stg_zones")
        with cursor.copy("COPY staging.stg_zones FROM STDIN (FORMAT csv, HEADER true)") as copy:
            copy.write(zone_csv.read_text())

        locations = run_sql_file(conn, "transform/load_dim_location.sql")
        dates = run_sql_file(
            conn,
            "transform/load_dim_date.sql",
            {"month_start": month_start, "month_end": month_end},
        )

    return {"dim_location_upserted": locations, "dim_date_inserted": dates}




def load_fact(dsn: str, month: str) -> int:
    """Rebuild the fact partition for ``month`` from staging. Returns rows inserted.

    Create-partition, truncate and insert run in ONE transaction, which makes
    the load idempotent and atomic: a retry or re-run replaces the month instead
    of duplicating it, and readers never see a half-loaded month.
    """
    month_start, month_end = config.month_bounds(month)
    partition = sql.Identifier("dw", f"fact_taxi_trips_{month.replace('-', '_')}")

    with connect(dsn) as conn:
        conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {} PARTITION OF dw.fact_taxi_trips "
                "FOR VALUES FROM ({}) TO ({})"
            ).format(partition, sql.Literal(month_start), sql.Literal(month_end))
        )
        conn.execute(sql.SQL("TRUNCATE {}").format(partition))
        rows_inserted = run_sql_file(conn, "transform/load_fact.sql")
        # Fresh statistics so the planner picks good plans for dashboard queries.
        conn.execute(sql.SQL("ANALYZE {}").format(partition))

    return rows_inserted



def load_staging(dsn: str, clean_path: Path, source_file: str) -> int:
    """Replace staging.stg_trips with the cleaned parquet file. Returns rows loaded.

    COPY is PostgreSQL's bulk-load path and is far faster than INSERTs. The
    file is streamed in batches so memory use stays flat for any file size.
    """
    copy_statement = sql.SQL("COPY staging.stg_trips ({}) FROM STDIN (FORMAT csv)").format(
        sql.SQL(", ").join(map(sql.Identifier, STAGING_COLUMNS))
    )
    rows_loaded = 0

   
    with connect(dsn) as conn, conn.cursor() as cursor:
        cursor.execute("TRUNCATE staging.stg_trips")
        with cursor.copy(copy_statement) as copy:
            for batch in pq.ParquetFile(clean_path).iter_batches(batch_size=config.COPY_BATCH_ROWS):
                frame = batch.to_pandas()
                # Nullable ints so 1 is written as "1" (not "1.0") and null as empty.
                frame = frame.astype({column: "Int64" for column in INTEGER_COLUMNS})
                frame["source_file"] = source_file

                buffer = io.StringIO()
                frame[STAGING_COLUMNS].to_csv(buffer, index=False, header=False)
                copy.write(buffer.getvalue())
                rows_loaded += len(frame)
        cursor.execute("ANALYZE staging.stg_trips")

    return rows_loaded


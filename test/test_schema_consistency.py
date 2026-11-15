"""Guards against drift between the Python schema and the SQL DDL (no database needed)."""

from __future__ import annotations

import re
from pathlib import Path

from schema import COLUMN_MAP, INTEGER_COLUMNS, STAGING_COLUMNS, WAREHOUSE_COLUMNS

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"


def table_columns(ddl: str, table: str) -> list[str]:
    """Column names of ``CREATE [UNLOGGED] TABLE IF NOT EXISTS <table> (...);``, in order."""
    pattern = rf"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+IF NOT EXISTS\s+{re.escape(table)}\s*\((.*?)\n\);"
    match = re.search(pattern, ddl, re.S)
    assert match, f"{table} not found in the DDL"
    columns = []
    for line in match.group(1).splitlines():
        line = line.split("--")[0].strip()
        if line:
            columns.append(line.split()[0])
    return columns


def test_staging_columns_match_staging_table_in_order():
    # load_staging COPYs STAGING_COLUMNS positionally, so order matters too.
    assert STAGING_COLUMNS == table_columns(SCHEMA_SQL.read_text(), "staging.stg_trips")


def test_drift_is_detected():
    ddl = SCHEMA_SQL.read_text()
    start = ddl.index("staging.stg_trips")  # the fact table also has airport_fee, so edit only staging
    drifted = ddl[:start] + ddl[start:].replace("airport_fee ", "airport_feeX", 1)
    assert STAGING_COLUMNS != table_columns(drifted, "staging.stg_trips")


def test_staging_columns_are_warehouse_columns_plus_source_file():
    assert STAGING_COLUMNS == [*WAREHOUSE_COLUMNS, "source_file"]


def test_integer_columns_are_known_warehouse_columns():
    assert set(INTEGER_COLUMNS) <= set(WAREHOUSE_COLUMNS)


def test_column_map_has_no_duplicate_targets():
    assert len(set(COLUMN_MAP.values())) == len(COLUMN_MAP)

"""Every setting of the pipeline, plus small date helpers.

Each value can be overridden with the environment variable of the same name
(from ``.env`` or the process environment); the defaults below apply otherwise.
"""

from __future__ import annotations

import os
import re
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# On the host the repository root is the parent of this package. Load its .env
# first; values already in the environment (e.g. from docker compose's env_file)
# are never overridden.
_REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_REPO_ROOT / ".env")

# Inside the Airflow containers PROJECT_ROOT is set explicitly.
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", _REPO_ROOT))


def _project_path(name: str, default: str) -> Path:
    """Read a directory setting from the environment; relative paths are
    resolved against PROJECT_ROOT, absolute paths are used as given."""
    return PROJECT_ROOT / os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


# ------------------------------------------------------------------- paths
RAW_DIR = _project_path("RAW_DIR", "data/raw")
CLEAN_DIR = _project_path("CLEAN_DIR", "data/clean")
SQL_DIR = _project_path("SQL_DIR", "sql")

TLC_BASE_URL = os.environ.get("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net").rstrip("/")
ZONE_LOOKUP_URL = os.environ.get("ZONE_LOOKUP_URL", f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv")
ZONE_LOOKUP_PATH = RAW_DIR / "taxi_zone_lookup.csv"


# -------------------------------------------------------------- cleaning rules
MAX_TRIP_HOURS = _env_int("MAX_TRIP_HOURS", 24)
MAX_TRIP_MILES = _env_int("MAX_TRIP_MILES", 200)
MAX_LOCATION_ID = _env_int("MAX_LOCATION_ID", 265)  # TLC taxi zones are numbered 1..265

# ------------------------------------------------------------- quality checks
MAX_UNKNOWN_PAYMENT_SHARE = _env_float("MAX_UNKNOWN_PAYMENT_SHARE", 0.10)

# ------------------------------------------------------------------- warehouse
WAREHOUSE_CONN_ID = os.environ.get("WAREHOUSE_CONN_ID", "nyc_taxi_dw")  # Airflow Connection ID
APPLICATION_NAME = os.environ.get("APPLICATION_NAME", "nyc_taxi_etl")
DB_CONNECT_TIMEOUT_S = _env_int("DB_CONNECT_TIMEOUT_S", 10)
# statement_timeout: a runaway or orphaned statement is cancelled by the server.
# lock_timeout: a step that cannot get its lock fails fast and is retried.
DB_STATEMENT_TIMEOUT = os.environ.get("DB_STATEMENT_TIMEOUT", "15min")
DB_LOCK_TIMEOUT = os.environ.get("DB_LOCK_TIMEOUT", "60s")
COPY_BATCH_ROWS = _env_int("COPY_BATCH_ROWS", 200_000)

# ------------------------------------------------------------ extract / alerts
DOWNLOAD_TIMEOUT_S = _env_int("DOWNLOAD_TIMEOUT_S", 120)
ALERT_WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")
ALERT_TIMEOUT_S = _env_int("ALERT_TIMEOUT_S", 10)

# --------------------------------------------------------------------- logging
LOG_LEVEL = os.environ.get("ETL_LOG_LEVEL", "INFO")
LOG_DIR = os.environ.get("ETL_LOG_DIR", "")  # empty = no log file

# ------------------------------------------------------------------ DAG / Airflow
DAG_SCHEDULE = os.environ.get("DAG_SCHEDULE", "@monthly")
DAG_START_DATE = os.environ.get("DAG_START_DATE", "2023-01-01")
DAG_END_DATE = os.environ.get("DAG_END_DATE", "2023-02-28")  # extend to load more months
DAG_CATCHUP = _env_bool("DAG_CATCHUP", True)
DAG_MAX_ACTIVE_RUNS = _env_int("DAG_MAX_ACTIVE_RUNS", 1)  # months share the staging table
TASK_RETRIES = _env_int("TASK_RETRIES", 2)
TASK_RETRY_DELAY_S = _env_int("TASK_RETRY_DELAY_S", 30)
TASK_MAX_RETRY_DELAY_MIN = _env_int("TASK_MAX_RETRY_DELAY_MIN", 5)
TASK_EXECUTION_TIMEOUT_MIN = _env_int("TASK_EXECUTION_TIMEOUT_MIN", 30)


_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def validate_month(month: str) -> str:
    """Return ``month`` if it is a valid ``YYYY-MM`` string, else raise ValueError."""
    if not _MONTH_PATTERN.match(month):
        raise ValueError(f"Invalid month {month!r}, expected YYYY-MM")
    return month


def month_bounds(month: str) -> tuple[date, date]:
    """Return (first day of the month, first day of the next month)."""
    validate_month(month)
    year, month_number = (int(part) for part in month.split("-"))
    start = date(year, month_number, 1)
    end = date(year + 1, 1, 1) if month_number == 12 else date(year, month_number + 1, 1)
    return start, end


def raw_trip_filename(month: str) -> str:
    return f"yellow_tripdata_{validate_month(month)}.parquet"


def raw_trip_path(month: str) -> Path:
    return RAW_DIR / raw_trip_filename(month)


def raw_trip_url(month: str) -> str:
    return f"{TLC_BASE_URL}/trip-data/{raw_trip_filename(month)}"



def clean_trip_path(month: str) -> Path:
    return CLEAN_DIR / f"yellow_tripdata_clean_{validate_month(month)}.parquet"

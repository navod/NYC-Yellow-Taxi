"""
### NYC Yellow Taxi - monthly batch ETL

Loads one month of TLC Yellow Taxi trips into the PostgreSQL star schema.

`create_schema -> extract -> clean -> load_staging -> load_dimensions -> load_fact
-> run_quality_checks -> summarise_run`
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pendulum

from airflow.sdk import BaseHook, Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from psycopg.conninfo import make_conninfo

from etl.audit import latest_step_results, tracked_step

from etl import config
from etl.alerts import send_failure_alert
from etl.load import create_schema as create_warehouse_schema
from etl.load import load_dimensions as load_warehouse_dimensions
from etl.load import load_fact as load_warehouse_fact

from etl.load import load_staging as load_warehouse_staging

from etl.utils.logging_utils import get_logger
from etl.extract import ensure_raw_trips, ensure_zone_lookup
from etl.clean import clean_trips
from etl.quality import run_quality_checks as run_warehouse_quality_checks


logger = get_logger("dag.nyc_taxi_etl")

WAREHOUSE_CONN_ID = config.WAREHOUSE_CONN_ID


PIPELINE_STEPS = [
    "create_schema",
    "clean",
    "load_staging",
    "load_dimensions",
    "load_fact",
    "run_quality_checks",
]


# --------------------------------------------------------------------- helpers
def warehouse_dsn() -> str:
    """Build a libpq connection string from the Airflow Connection `taxi_dw`.

    Credentials live in Airflow (here: the AIRFLOW_CONN_TAXI_DW env var), not in code.
    """
    conn = BaseHook.get_connection(WAREHOUSE_CONN_ID)
    return make_conninfo(
        host=conn.host,
        port=conn.port or 5432,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )


def step(name: str, month: str):
    """Open a tracked step bound to the current Airflow run."""
    run_id = get_current_context()["run_id"]
    return tracked_step(name, run_id=run_id, month=month, dsn=warehouse_dsn())





def alert_on_failure(context) -> None:
    """Task-level callback: runs once a task has failed and has no retries left."""
    task_instance = context["task_instance"]
    send_failure_alert(
        {
            "dag_id": task_instance.dag_id,
            "task_id": task_instance.task_id,
            "run_id": context["run_id"],
            "try_number": task_instance.try_number,
            "error": repr(context.get("exception")),
        }
    )


def log_retry(context) -> None:
    """Task-level callback: runs each time a failed task is scheduled for a retry."""
    task_instance = context["task_instance"]
    logger.warning(
        "task failed, will retry",
        extra={
            "task_id": task_instance.task_id,
            "run_id": context["run_id"],
            "try_number": task_instance.try_number,
            "error": repr(context.get("exception")),
        },
    )



DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Transient problems (network, database restart) get two more attempts,
    # waiting 30 s, then 60 s.
    "retries": config.TASK_RETRIES,
    "retry_delay": timedelta(seconds=config.TASK_RETRY_DELAY_S),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=config.TASK_MAX_RETRY_DELAY_MIN),
    # A hung task is killed instead of blocking the pipeline forever.
    "execution_timeout": timedelta(minutes=config.TASK_EXECUTION_TIMEOUT_MIN),
    "on_failure_callback": alert_on_failure,
    "on_retry_callback": log_retry,
}


# ------------------------------------------------------------------------- DAG
@dag(
    dag_id="nyc_taxi_etl",
    description="Monthly batch ETL of NYC Yellow Taxi trips into a PostgreSQL star schema",
    schedule=config.DAG_SCHEDULE,
    start_date=pendulum.parse(config.DAG_START_DATE, tz="UTC"),
    end_date=pendulum.parse(config.DAG_END_DATE, tz="UTC"),
    catchup=config.DAG_CATCHUP,
    max_active_runs=config.DAG_MAX_ACTIVE_RUNS,
    default_args=DEFAULT_ARGS,
    params={
        "month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Month to load as YYYY-MM. Leave empty to use the run's logical date.",
        )
    },
    tags=["nyc-taxi", "etl", "star-schema"],
    doc_md=__doc__,
)


def nyc_taxi_etl():
    @task
    def resolve_month() -> str:
        """Decide which data month this run processes."""
        context = get_current_context()
        month = context["params"].get("month")
        if not month:
            logical_date = context.get("logical_date")
            if logical_date is None:
                raise AirflowFailException("Manual runs need the 'month' param (YYYY-MM)")
            month = logical_date.strftime("%Y-%m")
        logger.info(
            "pipeline run started", extra={"run_id": context["run_id"], "data_month": month}
        )
        return config.validate_month(month)

    @task
    def create_schema(month: str) -> None:
        """Create the star schema if it does not exist and seed static dimensions."""
        with step("create_schema", month):
            create_warehouse_schema(warehouse_dsn())

    @task
    def extract(month: str) -> dict:
        """Make sure the raw parquet file is present and readable."""
        with step("extract", month) as metrics:
            path, row_count = ensure_raw_trips(month)
            ensure_zone_lookup()
            metrics.rows_out = row_count
            metrics.details = {"file": path.name, "size_mb": round(path.stat().st_size / 1e6, 1)}
        # XCom carries only small metadata; the data itself stays on disk.
        return {"raw_path": str(path), "raw_rows": row_count}


    @task
    def clean(month: str, extracted: dict) -> dict:
        """Apply the cleaning rules and write the result as parquet."""
        with step("clean", month) as metrics:
            raw = pd.read_parquet(extracted["raw_path"])
            cleaned, rejects = clean_trips(raw, month)
 
            clean_path = config.clean_trip_path(month)
            clean_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned.to_parquet(clean_path, index=False)
 
            metrics.rows_in = len(raw)
            metrics.rows_out = len(cleaned)
            metrics.rows_rejected = len(raw) - len(cleaned)
            metrics.details = {"rejects_by_rule": rejects}
        return {"clean_path": str(clean_path), "clean_rows": len(cleaned)}

    @task
    def load_dimensions(month: str) -> None:
        """Refresh dim_location and extend dim_date for the month."""
        with step("load_dimensions", month) as metrics:
            metrics.details = load_warehouse_dimensions(
                warehouse_dsn(), config.ZONE_LOOKUP_PATH, month
            )

    @task
    def load_staging(month: str, cleaned: dict) -> int:
        """Bulk-copy the cleaned file into staging.stg_trips."""
        with step("load_staging", month) as metrics:
            rows = load_warehouse_staging(
                warehouse_dsn(), config.clean_trip_path(month), config.raw_trip_filename(month)
            )
            metrics.rows_in = cleaned["clean_rows"]
            metrics.rows_out = rows
        return rows

    @task
    def load_fact(month: str, staged_rows: int) -> int:
        """Rebuild the month's fact partition from staging."""
        with step("load_fact", month) as metrics:
            rows = load_warehouse_fact(warehouse_dsn(), month)
            metrics.rows_in = staged_rows
            metrics.rows_out = rows
        return rows

    @task(retries=0)
    def run_quality_checks(month: str, staged_rows: int) -> None:
        """Validate the loaded month; raises if a check fails."""
        with step("run_quality_checks", month) as metrics:
            results = run_warehouse_quality_checks(warehouse_dsn(), month, staged_rows)
            metrics.details = {
                r.name: {"observed": r.observed, "passed": r.passed} for r in results
            }

    @task(trigger_rule="all_done", retries=0)
    def summarise_run(month: str) -> None:
        """Log one end-of-run summary and make the run's final state truthful."""
        run_id = get_current_context()["run_id"]
        results = {row["step"]: row for row in latest_step_results(warehouse_dsn(), run_id)}
        not_successful = [
            name for name in PIPELINE_STEPS if results.get(name, {}).get("status") != "success"
        ]
        logger.info(
            "pipeline run finished",
            extra={
                "run_id": run_id,
                "data_month": month,
                "status": "failed" if not_successful else "success",
                "steps": list(results.values()),
                "not_successful": not_successful,
            },
        )
        # This task is the DAG's only leaf, and Airflow derives the run state
        # from leaf tasks. Without this, a run with a failed step would show as
        # "success" simply because the summary itself ran fine.
        if not_successful:
            raise AirflowFailException(f"Steps not successful: {not_successful}")
    
    
    month = resolve_month()
    schema_ready = create_schema(month)
    extracted = extract(month)
    cleaned = clean(month, extracted)
    staged_rows = load_staging(month, cleaned)
    dimensions_ready = load_dimensions(month)
    fact_rows = load_fact(month, staged_rows)
    checked = run_quality_checks(month, staged_rows)


    # The schema comes first: every later step writes its audit row into it.
    # `extract` also fetches the zone lookup file that load_dimensions reads.
    schema_ready >> extracted >> dimensions_ready
    dimensions_ready >> fact_rows >> checked >> summarise_run(month)


nyc_taxi_etl()

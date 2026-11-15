from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from etl import config
from etl.utils.logging_utils import get_logger

logger = get_logger(__name__)

_INSERT_AUDIT_SQL = """
    INSERT INTO dw.etl_run_audit (
        run_id, data_month, step, status, started_at, finished_at, duration_s,
        rows_in, rows_out, rows_rejected, details, error_message
    ) VALUES (
        %(run_id)s, %(data_month)s, %(step)s, %(status)s, %(started_at)s, %(finished_at)s,
        %(duration_s)s, %(rows_in)s, %(rows_out)s, %(rows_rejected)s, %(details)s,
        %(error_message)s
    )
"""

_LATEST_STATUS_SQL = """
    SELECT DISTINCT ON (step)
        step, status, duration_s, rows_in, rows_out, rows_rejected, error_message
    FROM dw.etl_run_audit
    WHERE run_id = %(run_id)s
    ORDER BY step, started_at DESC
"""


@dataclass
class StepMetrics:
    """Mutable holder a step fills in while it runs."""

    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@contextmanager
def tracked_step(step: str, *, run_id: str, month: str, dsn: str) -> Iterator[StepMetrics]:
    """Time a pipeline step, log its outcome and record it in the audit table."""
    metrics = StepMetrics()
    context = {"step": step, "run_id": run_id, "data_month": month}
    started_at = datetime.now(timezone.utc)
    timer_start = time.perf_counter()
    logger.info("step started", extra={**context, "started_at": started_at})

    status, error_message = "success", None
    try:
        yield metrics
    except BaseException as exc:  # includes Airflow's task-timeout exception
        status, error_message = "failed", f"{type(exc).__name__}: {exc}"
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        duration_s = round(time.perf_counter() - timer_start, 2)
        outcome = {
            **context,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_s": duration_s,
            "rows_in": metrics.rows_in,
            "rows_out": metrics.rows_out,
            "rows_rejected": metrics.rows_rejected,
            "details": metrics.details,
            "error_message": error_message,
        }
        if status == "success":
            logger.info("step finished", extra=outcome)
        else:
            # exc_info=True adds the full traceback to the JSON record.
            logger.error("step failed", extra=outcome, exc_info=True)
        _write_audit_row(dsn, outcome)


def _write_audit_row(dsn: str, outcome: dict[str, Any]) -> None:
    """Persist the step outcome. Never raises: auditing must not hide the real error."""
    row = {**outcome, "details": json.dumps(outcome["details"], default=str)}
    try:
        with psycopg.connect(dsn, connect_timeout=config.DB_CONNECT_TIMEOUT_S) as conn:
            conn.execute(_INSERT_AUDIT_SQL, row)
    except psycopg.Error:
        logger.warning(
            "could not write audit row",
            extra={"step": outcome["step"], "run_id": outcome["run_id"]},
            exc_info=True,
        )


def latest_step_results(dsn: str, run_id: str) -> list[dict[str, Any]]:
    """Return the most recent audit row per step for one pipeline run."""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        return conn.execute(_LATEST_STATUS_SQL, {"run_id": run_id}).fetchall()

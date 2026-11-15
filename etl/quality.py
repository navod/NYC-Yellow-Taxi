"""Post-load data quality checks on the fact table for one month."""

from __future__ import annotations

from dataclasses import dataclass

from etl.config import MAX_UNKNOWN_PAYMENT_SHARE, month_bounds
from etl.load import connect

_MONTH_FILTER = "pickup_ts >= %(month_start)s AND pickup_ts < %(month_end)s"

_FACT_ROW_COUNT = f"SELECT count(*) FROM dw.fact_taxi_trips WHERE {_MONTH_FILTER}"

_INVALID_MEASURES = f"""
    SELECT count(*) FROM dw.fact_taxi_trips
    WHERE {_MONTH_FILTER}
      AND (trip_distance <= 0 OR fare_amount <= 0 OR total_amount <= 0
           OR trip_duration_minutes <= 0)
"""

_UNKNOWN_PAYMENT_SHARE = f"""
    SELECT COALESCE(avg((payment_type_key = 0)::int), 0) FROM dw.fact_taxi_trips
    WHERE {_MONTH_FILTER}
"""


class DataQualityError(Exception):
    """At least one data quality check failed."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    observed: float
    expectation: str


def run_quality_checks(dsn: str, month: str, expected_rows: int) -> list[CheckResult]:
    """Run all checks and return their results. Raises DataQualityError if any fail.

    ``expected_rows`` is the number of rows loaded into staging: if the fact
    holds fewer, the dimension joins silently dropped trips.
    """
    month_start, month_end = month_bounds(month)
    params = {"month_start": month_start, "month_end": month_end}

    with connect(dsn) as conn:
        fact_rows = conn.execute(_FACT_ROW_COUNT, params).fetchone()[0]
        invalid_measures = conn.execute(_INVALID_MEASURES, params).fetchone()[0]
        unknown_share = float(conn.execute(_UNKNOWN_PAYMENT_SHARE, params).fetchone()[0])

    results = [
        CheckResult("fact_not_empty", fact_rows > 0, fact_rows, "> 0 rows"),
        CheckResult(
            "row_count_matches_staging",
            fact_rows == expected_rows,
            fact_rows,
            f"== {expected_rows} staged rows",
        ),
        CheckResult("no_invalid_measures", invalid_measures == 0, invalid_measures, "== 0 rows"),
        CheckResult(
            "unknown_payment_share",
            unknown_share <= MAX_UNKNOWN_PAYMENT_SHARE,
            round(unknown_share, 4),
            f"<= {MAX_UNKNOWN_PAYMENT_SHARE}",
        ),
    ]

    failed = [result for result in results if not result.passed]
    if failed:
        summary = "; ".join(f"{r.name}: got {r.observed}, expected {r.expectation}" for r in failed)
        raise DataQualityError(f"Data quality checks failed for {month}: {summary}")
    return results

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from etl.config import MAX_LOCATION_ID, MAX_TRIP_HOURS, MAX_TRIP_MILES, month_bounds
from schema import COLUMN_MAP, INTEGER_COLUMNS, SchemaError


def standardise_columns(raw: pd.DataFrame) -> pd.DataFrame:
    lowered = raw.rename(columns=str.lower)
    missing = sorted(set(COLUMN_MAP) - set(lowered.columns))
    if missing:
        raise SchemaError(f"Raw data is missing expected columns: {missing}")
    return lowered[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)


def _build_rules(month: str) -> list[tuple[str, Callable[[pd.DataFrame], pd.Series]]]:
    """Return (rule name, function returning True for VALID rows), in order of application."""
    month_start, month_end = (pd.Timestamp(d) for d in month_bounds(month))
    max_duration = pd.Timedelta(hours=MAX_TRIP_HOURS)

    return [
        (
            "pickup_outside_month",
            lambda df: (df["pickup_ts"] >= month_start) & (df["pickup_ts"] < month_end),
        ),
        (
            "invalid_duration",
            lambda df: (
                (df["dropoff_ts"] > df["pickup_ts"])
                & (df["dropoff_ts"] - df["pickup_ts"] <= max_duration)
            ),
        ),
        (
            "invalid_distance",
            lambda df: (df["trip_distance"] > 0) & (df["trip_distance"] <= MAX_TRIP_MILES),
        ),
        (
            "non_positive_amount",
            lambda df: (df["fare_amount"] > 0) & (df["total_amount"] > 0),
        ),
        (
            "invalid_location",
            lambda df: (
                df["pickup_location_id"].between(1, MAX_LOCATION_ID)
                & df["dropoff_location_id"].between(1, MAX_LOCATION_ID)
            ),
        ),
    ]


def clean_trips(raw: pd.DataFrame, month: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Apply the cleaning rules to one month of raw trips.

    Returns the cleaned DataFrame and the number of rows rejected by each rule.
    Rules run in sequence, so a row is counted once - under the first rule it
    fails - and the reject counts add up to ``len(raw) - len(cleaned)``.
    """
    df = standardise_columns(raw)
    rejects: dict[str, int] = {}

    for rule_name, is_valid in _build_rules(month):
        valid = is_valid(df).fillna(False).astype(bool)
        rejects[rule_name] = int((~valid).sum())
        df = df[valid]

    before_dedup = len(df)
    df = df.drop_duplicates()
    rejects["duplicate"] = before_dedup - len(df)

   
    df = df.astype({column: "Int64" for column in INTEGER_COLUMNS})
    return df.reset_index(drop=True), rejects

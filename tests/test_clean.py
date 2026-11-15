"""Unit tests for the cleaning rules (no database or Airflow needed)."""

from __future__ import annotations

import pandas as pd
import pytest

from src.transform.clean_trips import SchemaError, clean_trips, standardise_columns
from src.config.settings import month_bounds, validate_month

MONTH = "2023-01"


def make_raw(**overrides) -> pd.DataFrame:
    """Build a one-row raw frame holding a valid trip, with optional overrides."""
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": pd.Timestamp("2023-01-15 08:00:00"),
        "tpep_dropoff_datetime": pd.Timestamp("2023-01-15 08:20:00"),
        "passenger_count": 1.0,
        "trip_distance": 3.5,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": 142,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 15.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 3.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 23.0,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_valid_trip_is_kept_and_renamed():
    cleaned, rejects = clean_trips(make_raw(), MONTH)

    assert len(cleaned) == 1
    assert sum(rejects.values()) == 0
    assert {"pickup_ts", "vendor_id", "pickup_location_id"} <= set(cleaned.columns)
    assert "store_and_fwd_flag" not in cleaned.columns


@pytest.mark.parametrize(
    ("overrides", "expected_rule"),
    [
        ({"tpep_pickup_datetime": pd.Timestamp("2022-12-31 23:59:00")}, "pickup_outside_month"),
        ({"tpep_dropoff_datetime": pd.Timestamp("2023-01-15 07:00:00")}, "invalid_duration"),
        ({"tpep_dropoff_datetime": pd.Timestamp("2023-01-17 08:00:00")}, "invalid_duration"),
        ({"trip_distance": 0.0}, "invalid_distance"),
        ({"trip_distance": 5000.0}, "invalid_distance"),
        ({"fare_amount": -15.0}, "non_positive_amount"),
        ({"total_amount": 0.0}, "non_positive_amount"),
        ({"PULocationID": 0}, "invalid_location"),
        ({"DOLocationID": 300}, "invalid_location"),
        ({"trip_distance": None}, "invalid_distance"),
    ],
)
def test_invalid_trip_is_rejected_by_expected_rule(overrides, expected_rule):
    cleaned, rejects = clean_trips(make_raw(**overrides), MONTH)

    assert cleaned.empty
    assert rejects[expected_rule] == 1
    assert sum(rejects.values()) == 1


def test_duplicates_are_removed_and_counted():
    raw = pd.concat([make_raw(), make_raw()], ignore_index=True)

    cleaned, rejects = clean_trips(raw, MONTH)

    assert len(cleaned) == 1
    assert rejects["duplicate"] == 1


def test_null_codes_are_kept_as_nullable_integers():
    raw = make_raw(passenger_count=None, RatecodeID=None)

    cleaned, _ = clean_trips(raw, MONTH)

    assert len(cleaned) == 1
    assert str(cleaned["passenger_count"].dtype) == "Int64"
    assert cleaned["rate_code_id"].isna().all()


def test_reject_counts_reconcile_with_row_counts():
    raw = pd.concat(
        [make_raw(), make_raw(trip_distance=0.0), make_raw(fare_amount=-1.0)], ignore_index=True
    )

    cleaned, rejects = clean_trips(raw, MONTH)

    assert len(raw) - len(cleaned) == sum(rejects.values())


def test_column_case_drift_is_absorbed():
    raw = make_raw().rename(columns={"airport_fee": "Airport_fee"})

    assert "airport_fee" in standardise_columns(raw).columns


def test_missing_column_raises_schema_error():
    with pytest.raises(SchemaError, match="fare_amount"):
        standardise_columns(make_raw().drop(columns=["fare_amount"]))


def test_month_bounds_handles_december():
    start, end = month_bounds("2023-12")

    assert (start.isoformat(), end.isoformat()) == ("2023-12-01", "2024-01-01")


@pytest.mark.parametrize("bad_month", ["2023-13", "2023-1", "23-01", "2023/01", ""])
def test_validate_month_rejects_bad_input(bad_month):
    with pytest.raises(ValueError):
        validate_month(bad_month)

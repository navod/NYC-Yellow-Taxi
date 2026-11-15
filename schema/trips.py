"""Trip data layout: single source of truth for the Python side.

The raw TLC column names, the warehouse column names and their types are
defined here once, so every pipeline step agrees on them.
"""

from __future__ import annotations

# Raw TLC column name (lower-cased) -> warehouse column name.
# Lower-casing first absorbs source drift such as `airport_fee` vs `Airport_fee`.
COLUMN_MAP = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "total_amount": "total_amount",
}

# Warehouse column names, in the order produced by the cleaning step.
WAREHOUSE_COLUMNS = list(COLUMN_MAP.values())

# Whole-number columns. The source stores some as floats because they contain
# nulls; pandas' nullable Int64 keeps them integers without losing the nulls.
INTEGER_COLUMNS = [
    "vendor_id",
    "passenger_count",
    "rate_code_id",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type_id",
]

# Columns of staging.stg_trips, in COPY order (the cleaned columns + lineage).
STAGING_COLUMNS = [*WAREHOUSE_COLUMNS, "source_file"]


class SchemaError(Exception):
    """The raw file does not contain the columns the pipeline expects."""

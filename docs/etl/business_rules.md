# Business rules

## Scope

- Dataset: NYC TLC **Yellow Taxi** trip records (parquet), plus the taxi zone lookup.
- Unit of load: one calendar month, by pickup time.

## Cleaning rules

Implemented in `src/transform/clean_trips.py`. Rules run **in this order**; a row is rejected by the first rule it fails, so each rejected row is counted once and the counts add up to `raw rows - clean rows`. The counts are stored in `dw.etl_run_audit.details->'rejects_by_rule'`.

| # | Rule | A row is rejected when | Setting |
|---|---|---|---|
| 1 | `pickup_outside_month` | pickup is not inside the month being loaded | |
| 2 | `invalid_duration` | drop-off is not after pickup, or the trip is longer than the limit | `MAX_TRIP_HOURS` (24) |
| 3 | `invalid_distance` | distance <= 0 or above the limit (miles) | `MAX_TRIP_MILES` (200) |
| 4 | `non_positive_amount` | `fare_amount` <= 0 or `total_amount` <= 0 | |
| 5 | `invalid_location` | pickup or drop-off zone outside 1..limit | `MAX_LOCATION_ID` (265) |
| 6 | `duplicate` | exact duplicate of an earlier row | |

Before the rules run, column names are lower-cased and mapped to the warehouse names (`src/schema/trips.py`). A missing expected column raises `SchemaError` and fails the step.

## Dimension keys

- `dim_vendor`, `dim_rate_code`, `dim_payment_type` hold an `Unknown` row with key 0. Unmatched codes map to 0, so the fact never has NULL foreign keys.
- Location joins are inner joins. A trip whose zone is missing from `dim_location` is dropped by the join, which the `row_count_matches_staging` check catches.

## Post-load quality checks

Implemented in `src/quality/checks.py`; run on the loaded month of `dw.fact_taxi_trips`. Any failure raises `DataQualityError` and fails the run.

| Check | Passes when |
|---|---|
| `fact_not_empty` | the month has > 0 fact rows |
| `row_count_matches_staging` | fact rows == rows loaded into staging (no trips lost in joins) |
| `no_invalid_measures` | no row has distance, fare, total or duration <= 0 |
| `unknown_payment_share` | share of payment type `Unknown` <= `MAX_UNKNOWN_PAYMENT_SHARE` (0.10) |

## Metric definitions

Dashboard queries live in `sql/reporting/`.

| Metric | Definition | File |
|---|---|---|
| Revenue | sum of `total_amount` | `revenue_by_payment_type.sql`, `kpi_summary.sql` |
| Average fare per mile | `sum(fare_amount) / sum(trip_distance)` (ratio of sums, so long trips weigh more) | `avg_fare_per_mile*.sql`, `kpi_summary.sql` |
| Peak hours | trip count and share by pickup hour (`dim_time`) | `peak_hours.sql` |
| Pipeline runs | the 50 most recent step rows from `dw.etl_run_audit` | `pipeline_runs.sql` |

# Entity-relationship diagram

Star schema in the `dw` schema, defined in `sql/schema.sql`. Grain of the fact table: **one row per completed taxi trip**. Full column list: [data_dictionary.csv](data_dictionary.csv).

```mermaid
erDiagram
    dim_date         ||--o{ fact_taxi_trips : "pickup_date_key"
    dim_time         ||--o{ fact_taxi_trips : "pickup_time_key"
    dim_location     ||--o{ fact_taxi_trips : "pickup_location_key"
    dim_location     ||--o{ fact_taxi_trips : "dropoff_location_key"
    dim_vendor       ||--o{ fact_taxi_trips : "vendor_key"
    dim_rate_code    ||--o{ fact_taxi_trips : "rate_code_key"
    dim_payment_type ||--o{ fact_taxi_trips : "payment_type_key"

    fact_taxi_trips {
        bigint trip_id PK
        timestamp pickup_ts PK "partition key (monthly)"
        int pickup_date_key FK
        smallint pickup_time_key FK
        smallint pickup_location_key FK
        smallint dropoff_location_key FK
        smallint vendor_key FK
        smallint rate_code_key FK
        smallint payment_type_key FK
        timestamp dropoff_ts
        smallint passenger_count
        numeric trip_distance
        numeric trip_duration_minutes
        numeric fare_amount
        numeric tip_amount
        numeric total_amount
        varchar source_file
    }
    dim_date {
        int date_key PK "YYYYMMDD"
        date full_date
        smallint year
        smallint month
        boolean is_weekend
    }
    dim_time {
        smallint time_key PK "hour 0-23"
        varchar day_part
        boolean is_rush_hour
    }
    dim_location {
        smallint location_key PK
        smallint location_id "TLC LocationID"
        varchar borough
        varchar zone
    }
    dim_vendor {
        smallint vendor_key PK
        smallint vendor_id
        varchar vendor_name
    }
    dim_rate_code {
        smallint rate_code_key PK
        smallint rate_code_id
        varchar rate_code_name
    }
    dim_payment_type {
        smallint payment_type_key PK
        smallint payment_type_id
        varchar payment_type_name
    }
```

The fact table also carries `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee` and `loaded_at` (omitted above for readability).

## Notes

- `dim_location` is a role-playing dimension: the fact references it twice, as pickup and drop-off.
- `dim_vendor`, `dim_rate_code` and `dim_payment_type` hold an `Unknown` row with key 0, so fact foreign keys are never NULL.
- `dw.fact_taxi_trips` is range-partitioned by month on `pickup_ts`; partitions are created by `src/load/load_to_db.py`.

## Outside the star

| Table | Role |
|---|---|
| `staging.stg_trips` | UNLOGGED landing table for cleaned trips, truncated every run |
| `staging.stg_zones` | UNLOGGED landing table for the zone lookup CSV |
| `dw.etl_run_audit` | One row per pipeline step attempt (monitoring trail) |

INSERT INTO dw.fact_taxi_trips (
    pickup_date_key, pickup_time_key, pickup_location_key, dropoff_location_key,
    vendor_key, rate_code_key, payment_type_key,
    pickup_ts, dropoff_ts,
    passenger_count, trip_distance, trip_duration_minutes,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, congestion_surcharge, airport_fee, total_amount,
    source_file
)
SELECT
    to_char(s.pickup_ts, 'YYYYMMDD')::int,
    extract(hour FROM s.pickup_ts)::smallint,
    pl.location_key,
    dl.location_key,
    COALESCE(v.vendor_key, 0),
    COALESCE(r.rate_code_key, 0),
    COALESCE(p.payment_type_key, 0),
    s.pickup_ts,
    s.dropoff_ts,
    s.passenger_count,
    s.trip_distance,
    round(extract(epoch FROM (s.dropoff_ts - s.pickup_ts)) / 60.0, 2),
    s.fare_amount,
    COALESCE(s.extra, 0),
    COALESCE(s.mta_tax, 0),
    COALESCE(s.tip_amount, 0),
    COALESCE(s.tolls_amount, 0),
    COALESCE(s.improvement_surcharge, 0),
    COALESCE(s.congestion_surcharge, 0),
    COALESCE(s.airport_fee, 0),
    s.total_amount,
    s.source_file
FROM staging.stg_trips AS s
JOIN dw.dim_location       AS pl ON pl.location_id     = s.pickup_location_id
JOIN dw.dim_location       AS dl ON dl.location_id     = s.dropoff_location_id
LEFT JOIN dw.dim_vendor       AS v ON v.vendor_id       = s.vendor_id
LEFT JOIN dw.dim_rate_code    AS r ON r.rate_code_id    = s.rate_code_id
LEFT JOIN dw.dim_payment_type AS p ON p.payment_type_id = s.payment_type_id;

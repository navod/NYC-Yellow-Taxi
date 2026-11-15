-- Headline numbers for the dashboard.
SELECT
    count(*)                                              AS total_trips,
    round(sum(total_amount), 2)                           AS total_revenue,
    round(sum(fare_amount) / sum(trip_distance), 2)       AS avg_fare_per_mile,
    round(avg(trip_distance), 2)                          AS avg_trip_miles,
    round(avg(trip_duration_minutes), 1)                  AS avg_trip_minutes,
    min(pickup_ts)::date                                  AS first_trip_date,
    max(pickup_ts)::date                                  AS last_trip_date
FROM dw.fact_taxi_trips;

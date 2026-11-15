-- Average fare per mile by pickup borough (same weighted definition).
SELECT
    l.borough,
    count(*)                                              AS trips,
    round(sum(f.fare_amount) / sum(f.trip_distance), 2)   AS avg_fare_per_mile
FROM dw.fact_taxi_trips AS f
JOIN dw.dim_location AS l ON l.location_key = f.pickup_location_key
GROUP BY l.borough
ORDER BY trips DESC;

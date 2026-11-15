-- Average fare amount per mile, per month.
--
-- Weighted average: total fare / total miles. Averaging each trip's own
-- fare-per-mile instead would let very short trips (e.g. $3 for 0.01 mi)
-- dominate the result.
SELECT
    d.year,
    d.month,
    d.month_name,
    count(*)                                              AS trips,
    round(sum(f.fare_amount) / sum(f.trip_distance), 2)   AS avg_fare_per_mile
FROM dw.fact_taxi_trips AS f
JOIN dw.dim_date AS d ON d.date_key = f.pickup_date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

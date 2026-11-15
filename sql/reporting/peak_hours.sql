-- Peak hours: trips per pickup hour of day, ranked busiest first.
SELECT
    t.time_key                                            AS pickup_hour,
    t.hour_label,
    t.day_part,
    count(*)                                              AS trips,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2)    AS pct_of_trips,
    rank() OVER (ORDER BY count(*) DESC)                  AS busiest_rank
FROM dw.fact_taxi_trips AS f
JOIN dw.dim_time AS t ON t.time_key = f.pickup_time_key
GROUP BY t.time_key, t.hour_label, t.day_part
ORDER BY t.time_key;

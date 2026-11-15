-- Total revenue by payment type. Revenue = total_amount (fare + taxes,
-- surcharges, tolls and card tips; cash tips are not recorded by TLC).
SELECT
    p.payment_type_name,
    count(*)                                                          AS trips,
    round(sum(f.total_amount), 2)                                     AS total_revenue,
    round(100.0 * sum(f.total_amount) / sum(sum(f.total_amount)) OVER (), 2) AS pct_of_revenue
FROM dw.fact_taxi_trips AS f
JOIN dw.dim_payment_type AS p ON p.payment_type_key = f.payment_type_key
GROUP BY p.payment_type_name
ORDER BY total_revenue DESC;

-- Static reference data from the TLC Yellow Taxi data dictionary.
-- ON CONFLICT DO NOTHING makes every statement safe to re-run.

INSERT INTO dw.dim_vendor (vendor_key, vendor_id, vendor_name) VALUES
    (0, -1, 'Unknown'),
    (1,  1, 'Creative Mobile Technologies, LLC'),
    (2,  2, 'Curb Mobility, LLC'),
    (3,  6, 'Myle Technologies INC'),
    (4,  7, 'Helix')
ON CONFLICT (vendor_key) DO NOTHING;

INSERT INTO dw.dim_rate_code (rate_code_key, rate_code_id, rate_code_name) VALUES
    (0, -1, 'Unknown'),
    (1,  1, 'Standard rate'),
    (2,  2, 'JFK'),
    (3,  3, 'Newark'),
    (4,  4, 'Nassau or Westchester'),
    (5,  5, 'Negotiated fare'),
    (6,  6, 'Group ride')
ON CONFLICT (rate_code_key) DO NOTHING;

INSERT INTO dw.dim_payment_type (payment_type_key, payment_type_id, payment_type_name) VALUES
    (0, -1, 'Unknown'),
    (1,  1, 'Credit card'),
    (2,  2, 'Cash'),
    (3,  3, 'No charge'),
    (4,  4, 'Dispute'),
    (5,  6, 'Voided trip'),
    (6,  0 , 'Flex Fare trip') 
ON CONFLICT (payment_type_key) DO NOTHING;

INSERT INTO dw.dim_time (time_key, hour_label, day_part, is_rush_hour)
SELECT
    h,
    lpad(h::text, 2, '0') || ':00 - ' || lpad(h::text, 2, '0') || ':59',
    CASE
        WHEN h < 6  THEN 'Night'
        WHEN h < 12 THEN 'Morning'
        WHEN h < 18 THEN 'Afternoon'
        ELSE 'Evening'
    END,
    h BETWEEN 7 AND 9 OR h BETWEEN 16 AND 19
FROM generate_series(0, 23) AS h
ON CONFLICT (time_key) DO NOTHING;

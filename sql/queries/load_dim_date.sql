INSERT INTO dw.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day_of_month, day_of_week, day_name, is_weekend
)
SELECT
    to_char(d, 'YYYYMMDD')::int,
    d::date,
    extract(year    FROM d)::smallint,
    extract(quarter FROM d)::smallint,
    extract(month   FROM d)::smallint,
    to_char(d, 'FMMonth'),
    extract(day     FROM d)::smallint,
    extract(isodow  FROM d)::smallint,
    to_char(d, 'FMDay'),
    extract(isodow  FROM d) >= 6
FROM generate_series(
    %(month_start)s::timestamp,
    %(month_end)s::timestamp - interval '1 day',
    interval '1 day'
) AS g(d)
ON CONFLICT (date_key) DO NOTHING;

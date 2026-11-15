-- Latest pipeline step results, newest first (monitoring panel).
SELECT
    data_month,
    step,
    status,
    started_at,
    duration_s,
    rows_in,
    rows_out,
    rows_rejected,
    error_message
FROM dw.etl_run_audit
ORDER BY started_at DESC
LIMIT 50;

# NYC Yellow Taxi - Batch ETL, Star Schema and Dashboard

A local, automated batch pipeline: it downloads NYC Yellow Taxi trip data, cleans it,
loads it into a PostgreSQL star schema, and exposes business metrics in an Apache
Superset dashboard. It is orchestrated by Apache Airflow and runs with Docker Compose.

Covers **Assignment 1** (Batch ETL, Data Modeling, SQL, Python, Local Orchestration, Basic Monitoring).

## Architecture

```
 TLC CloudFront (parquet)
        |  curl (scripts/setup.sh)  /  urllib (src/extract/fetch_data.py)
        v
 data/input/*.parquet
        |
        v                       Airflow DAG: nyc_taxi_etl  (one run = one month)
 +-------------------------------------------------------------------------+
 | resolve_month -> create_schema -> extract -> clean -> load_staging      |
 |                         \-> load_dimensions ----------\                 |
 |                                 load_fact -> run_quality_checks -> summarise_run
 +-------------------------------------------------------------------------+
        |  each step writes one row to dw.etl_run_audit (status, timings, row counts)
        v
 PostgreSQL 16
   staging.stg_trips        (UNLOGGED, truncated every run)
   dw.dim_* + dw.fact_taxi_trips   (star schema, fact partitioned by month)
   dw.etl_run_audit         (monitoring trail)
        |
        v
 Apache Superset  -->  dashboard "NYC Yellow Taxi - Trip Analytics"
```

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 3 (LocalExecutor) |
| Storage | PostgreSQL 16 (one instance: Airflow metadata, Superset metadata, warehouse) |
| ETL | Python 3.12, pandas, psycopg 3 (bulk `COPY`) |
| Dashboard | Apache Superset 6.1 |
| Packaging | Docker Compose, `Makefile`, `scripts/setup.sh` |

### Data model (`sql/schema.sql`)

Grain of the fact table: **one row per completed taxi trip**.

- **`dw.fact_taxi_trips`** - measures (distance, duration, fare, tip, tolls, surcharges, total) plus keys to the dimensions. Partitioned by month on `pickup_ts`.
- **Dimensions:** `dim_date`, `dim_time` (hour grain), `dim_location` (used twice, as pickup and drop-off), `dim_vendor`, `dim_rate_code`, `dim_payment_type`.
- Small dimensions contain an `Unknown` row with key 0, so the fact table never has NULL foreign keys.
- **`staging.stg_trips`** is an UNLOGGED landing table (faster bulk load, disposable data).
- **`dw.etl_run_audit`** records every step attempt.

### Key design decisions

| Decision | Why |
|---|---|
| Monthly fact partitions, rebuilt per run | Re-running a month is idempotent: that month's partition is truncated and reloaded, so no duplicates. |
| Cleaning in Python, joins and keys in SQL | Row-level rules are easy to unit test in pandas; set-based key lookups are fastest in Postgres. |
| Reject counts per rule | Rules run in order, so each rejected row is counted once, and the counts add up to `raw - clean`. |
| Bulk `COPY` into UNLOGGED staging | About 3M rows per month loads in roughly a minute. |
| Post-load quality checks | Fail the run if the fact is empty, row count differs from staging (silent join drops), measures are invalid, or too many payment types are Unknown. |
| `summarise_run` runs with `trigger_rule=all_done` and fails if any step failed | Airflow derives run state from leaf tasks, so without this a run with a failed step could show as success. |
| Analytics SQL lives in `sql/reporting/*.sql` | Superset datasets are built from these same files, so SQL is version controlled and reviewable. |

## Assumptions

1. Only the **Yellow Taxi** dataset is used, months 2023-01 and 2023-02 (the assignment asks for at least two).
2. A **month is the unit of load.** Pickups outside the month in the file are rejected.
3. Cleaning rules (thresholds configurable in `.env`):
   - pickup outside the month
   - drop-off not after pickup, or trip longer than 24 h
   - distance <= 0 or > 200 miles
   - fare or total amount <= 0
   - location ID outside 1-265
   - exact duplicate rows
4. Revenue is `total_amount`. Average fare per mile is `fare_amount / trip_distance`.
5. Airflow runs with `SIMPLE_AUTH_MANAGER_ALL_ADMINS` (no login) - **local demo only**.
6. Superset uses `SimpleCache` (5 min) - fine for one local process; production would use Redis.
7. All credentials come from `.env` (gitignored). `docker-compose.yml` has no credential defaults and refuses to start if a required variable is missing.

## Quick start

Prerequisites: Docker with Compose v2, `curl`, and `uv` or Python 3.10+.

```bash
make setup                      # venv, dependencies, data download, docker compose up
```

`scripts/setup.sh` copies `.env.example` to `.env` if there is none. **Edit `.env` and set real
passwords and `SUPERSET_SECRET_KEY` before first start** (e.g. `openssl rand -base64 42`).

Then:

```bash
# 1. Unpause the DAG; the scheduler runs the months inside the DAG window
make unpause

# 2. Create the Superset database connection, datasets, charts and dashboard (idempotent)
make dashboard
```

| Service | URL |
|---|---|
| Airflow | http://localhost:8081 |
| Superset dashboard | http://localhost:8088/superset/dashboard/nyc-taxi/ (login from `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD`) |
| PostgreSQL | localhost:`DW_PORT` |

### Loading more months

The DAG only runs inside `DAG_START_DATE`..`DAG_END_DATE` (defaults `2023-01-01`..`2023-02-28`).
Airflow skips runs outside that window, even manual ones. To load more, raise `DAG_END_DATE`
in `.env`, recreate the Airflow containers, and trigger a run with `month=YYYY-MM`
(or backfill). The month's file must exist on the TLC site. A month is loaded by its
logical date, so the run for month M fires on the first day of month M+1 only if
`DAG_END_DATE` is after that.

## Monitoring and logging

- **Structured logging:** `src/config/logging_config.py` emits JSON log lines with run id, data month, row counts and errors (also to `pipeline.jsonl` in `ETL_LOG_DIR`, e.g. `tail -f logs/etl/pipeline.jsonl | jq`).
- **Audit table:** every step writes status, start/end, duration, `rows_in`, `rows_out`, `rows_rejected`, per-rule reject counts and the error message to `dw.etl_run_audit`. The dashboard shows it.
- **Error handling:** explicit exceptions (`SchemaError`, `DataQualityError`); retries with exponential backoff (`TASK_RETRIES`, default 2) and an execution timeout per task.
- **Alerts:** `on_failure_callback` sends a Slack-compatible webhook message if `ALERT_WEBHOOK_URL` is set; `on_retry_callback` logs each retry.

## Dashboard metrics

| Metric | File |
|---|---|
| Average fare per mile (overall, by month, by pickup borough) | `sql/reporting/avg_fare_per_mile*.sql` |
| Peak hours for rides | `sql/reporting/peak_hours.sql` |
| Total revenue by payment type | `sql/reporting/revenue_by_payment_type.sql` |
| KPI tiles (trips, revenue, avg fare per mile, avg distance) | `sql/reporting/kpi_summary.sql` |
| Pipeline run history | `sql/reporting/pipeline_runs.sql` |

## Repository layout

```
Makefile                     common tasks: make setup | up | down | test | docs | dashboard (make help)
.env.example                 configuration template, copied to .env by setup
.dockerignore                keeps data/, logs/ and .venv out of the Docker build context
data/
  input/                     raw TLC parquet files and zone lookup (downloaded)
  output/                    cleaned parquet files, one per month
Docker/
  Dockerfile_airflow         Airflow image + requirements/prod.txt
  Dockerfile_superset        Superset image + PostgreSQL driver
docker-compose.yml           Postgres, Airflow, Superset
dags/nyc_taxi_etl.py         Airflow DAG (Task 3)
docs/                        technical docs: deployment, ETL flow, business rules, ER diagram, data dictionary
logs/                        Airflow task logs and logs/etl/pipeline.jsonl (runtime)
notebooks/main.ipynb         raw data exploration
requirements/                base.txt (runtime), dev.txt (+ pytest, ipykernel), prod.txt (Airflow image)
scripts/
  setup.sh                   one-shot setup (Task 1)
  run_tests.sh               run the test suite
  init-databases.sh          Postgres first-start hook: creates the databases
  generate_data_dictionary.py  builds docs/schema/data_dictionary.csv from sql/schema.sql
sql/
  schema.sql                 star schema (Task 2)
  seed_dimensions.sql        static dimension rows
  queries/                   dimension and fact load SQL
  reporting/                 dashboard queries (Task 5)
src/                         ETL code (Tasks 3-4)
  config/                    settings.py (all settings), logging_config.py (JSON logging)
  db_connection/             postgres.py (connections), reader.py (run SQL files)
  extract/fetch_data.py      download and validate raw files
  transform/clean_trips.py   cleaning rules with per-rule reject counts
  load/load_to_db.py         COPY into staging, dimension and fact loads
  quality/checks.py          post-load data quality checks
  schema/trips.py            column contract shared with the DDL
  utils/                     audit.py (dw.etl_run_audit), alerts.py (failure webhook)
superset/                    Superset config and dashboard-as-code bootstrap
tests/                       unit tests: cleaning rules, DAG integrity, schema consistency
```

Run the tests with `make test`. More detail is in [docs/](docs/README.md).

## Demo script (about 10 minutes)

Stage by stage, so it still works if not everything runs at once.

**1. Setup (Task 1) - 1 min**
- Show [scripts/setup.sh](scripts/setup.sh): venv creation, `curl` downloads with retry and `.part` rename, `docker compose up --wait`.
- `ls data/input` shows two monthly parquet files and the zone lookup.
- `docker compose ps` shows Postgres, Airflow and Superset healthy.

**2. Data model (Task 2) - 2 min**
- Open [sql/schema.sql](sql/schema.sql). Explain the grain, the role-playing `dim_location`, the Unknown rows, and the monthly partitioning.
- In `psql`: `\dt dw.*` and `\d dw.fact_taxi_trips`.

**3. Pipeline (Task 3) - 3 min**
- In the Airflow UI open `nyc_taxi_etl`, show the graph and a successful run.
- Walk through [src/transform/clean_trips.py](src/transform/clean_trips.py): the ordered rules and per-rule reject counts.
- Show [src/load/load_to_db.py](src/load/load_to_db.py) `load_fact`: create partition, truncate, insert, which makes reruns idempotent.
- Prove idempotency: clear the 2023-02 run, and show the fact row count for February is unchanged:
  ```sql
  SELECT to_char(pickup_ts,'YYYY-MM') AS month, count(*) FROM dw.fact_taxi_trips GROUP BY 1 ORDER BY 1;
  -- 2023-01: 2,998,181    2023-02: 2,850,138
  ```

**4. Monitoring (Task 4) - 2 min**
- `SELECT step, status, duration_s, rows_in, rows_out, rows_rejected FROM dw.etl_run_audit ORDER BY started_at DESC LIMIT 10;`
- Show the February reject breakdown:
  `SELECT details->'rejects_by_rule' FROM dw.etl_run_audit WHERE step='clean' AND data_month='2023-02' ORDER BY started_at DESC LIMIT 1;`
- Show [src/quality/checks.py](src/quality/checks.py) and the failure and retry callbacks in the DAG.
- **Failure demo:** set `MAX_TRIP_MILES=0` temporarily or point `TLC_BASE_URL` at a bad URL, trigger a run, and show the failed task, the retry log, the audit row with `error_message`, and `summarise_run` failing the run. Revert afterwards.

**5. Dashboard (Task 5) - 2 min**
- Open the Superset dashboard: KPI tiles, peak hours, revenue by payment type, fare per mile by borough and by month, pipeline runs table.
- Open one chart's SQL and show it is the file in `sql/reporting/`.

### Questions to be ready for

- *Why partition the fact table?* Month-filtered queries scan one partition, and a rerun replaces exactly one.
- *Why is staging UNLOGGED?* It is disposable, and skipping the write-ahead log speeds up `COPY`.
- *What if a dimension lookup fails?* Location joins are inner joins, so the `row_count_matches_staging` check fails the run instead of silently dropping trips. Vendor, rate code and payment type fall back to key 0.
- *Why `max_active_runs=1`?* All months share the staging table.
- *How would this scale?* Replace LocalExecutor with Celery or Kubernetes, use a managed warehouse, and swap staging `COPY` for object-store loads.

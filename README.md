# NYC Yellow Taxi - Batch ETL, Star Schema and Dashboard

A local, automated batch pipeline that downloads NYC Yellow Taxi trip data, cleans it, loads it
into a PostgreSQL star schema, and shows business metrics in an Apache Superset dashboard.
Apache Airflow orchestrates the pipeline, and everything runs in Docker Compose.

- [Technology](#technology)
- [Architecture](#architecture)
- [Getting started](#getting-started) - [macOS](#macos) | [Linux](#linux) | [Windows](#windows)
- [Using the project](#using-the-project)
- [Configuration](#configuration)
- [Monitoring and logging](#monitoring-and-logging)
- [Project structure](#project-structure)
- [Troubleshooting](#troubleshooting)

More detailed documentation is in [docs/](docs/README.md).

## Technology

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 3.4 (LocalExecutor) |
| Storage | PostgreSQL 16, one instance for the warehouse, Airflow metadata and Superset metadata |
| ETL | Python 3.12, pandas, pyarrow, psycopg 3 (bulk `COPY`) |
| Dashboard | Apache Superset 6.1, built as code through its REST API |
| Packaging | Docker Compose, `Makefile`, `scripts/setup.sh` |
| Testing | pytest; GitHub Actions CI |
| Data source | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Yellow Taxi parquet files and the taxi zone lookup) |

## Architecture

```
 NYC TLC (parquet over HTTPS)
        |  scripts/setup.sh (curl)  or  the extract task (src/extract/fetch_data.py)
        v
 data/input/*.parquet ──clean──> data/output/*.parquet
        |
        v                         Airflow DAG: nyc_taxi_etl  (one run = one month)
 +----------------------------------------------------------------------------+
 | resolve_month -> create_schema -> extract -> clean -> load_staging          |
 |                                       \-> load_dimensions --\               |
 |                                    load_fact -> run_quality_checks -> summarise_run
 +----------------------------------------------------------------------------+
        |  every step writes one row to dw.etl_run_audit (status, timings, row counts)
        v
 PostgreSQL 16
   staging.stg_trips              UNLOGGED landing table, truncated every run
   dw.dim_* + dw.fact_taxi_trips  star schema, fact partitioned by month
   dw.etl_run_audit               monitoring trail
        |
        v
 Apache Superset  -->  dashboard "NYC Yellow Taxi - Trip Analytics"
```

### Containers

| Service | Image | Port (host) |
|---|---|---|
| `postgres` | `postgres:16` | 5433 (`DW_PORT`) |
| `airflow-apiserver` | `Docker/Dockerfile_airflow` | 8081 (`AIRFLOW_PORT`) |
| `airflow-scheduler`, `airflow-dag-processor` | `Docker/Dockerfile_airflow` | - |
| `airflow-init` | `Docker/Dockerfile_airflow` (runs `db migrate`, then exits) | - |
| `superset` | `Docker/Dockerfile_superset` | 8088 (`SUPERSET_PORT`) |

### Data model

Defined in [sql/schema.sql](sql/schema.sql); see the [ER diagram](docs/schema/er_diagram.md) and [data dictionary](docs/schema/data_dictionary.csv).
Grain of the fact table: **one row per completed taxi trip**.

- **`dw.fact_taxi_trips`**: measures (distance, duration, fare, tip, tolls, surcharges, total) and keys to the dimensions, partitioned by month on `pickup_ts`.
- **Dimensions:** `dim_date`, `dim_time` (hour grain), `dim_location` (used twice, for pickup and drop-off), `dim_vendor`, `dim_rate_code`, `dim_payment_type`.
- The small dimensions have an `Unknown` row with key 0, so the fact table never has NULL foreign keys.

### Key design decisions

| Decision | Why |
|---|---|
| Monthly fact partitions, rebuilt on every run | Re-running a month is idempotent: its partition is truncated and reloaded in one transaction, so there are no duplicates. |
| Cleaning in Python, joins and keys in SQL | Row-level rules are easy to unit test in pandas; set-based key lookups are fastest in Postgres. |
| Reject counts per rule | Rules run in order, so each rejected row is counted once and the counts add up to `raw - clean`. |
| Bulk `COPY` into an UNLOGGED staging table | About 3M rows per month load in roughly a minute. |
| Post-load quality checks | The run fails if the fact is empty, its row count differs from staging (rows silently dropped by joins), a measure is invalid, or too many payment types are Unknown. |
| `summarise_run` uses `trigger_rule=all_done` and fails if any step failed | Airflow takes the run state from leaf tasks, so without it a run with a failed step could show as successful. |
| Dashboard SQL lives in `sql/reporting/` | Superset datasets are built from these files, so the SQL is version controlled and reviewable. |

The cleaning rules, quality checks and metric definitions are in [docs/etl/business_rules.md](docs/etl/business_rules.md).
The task order, schedule and retry behaviour are in [docs/etl/pipeline_flow.md](docs/etl/pipeline_flow.md).

## Getting started

### Requirements (all platforms)

| Requirement | Notes |
|---|---|
| Docker with Compose v2 | Docker Desktop on macOS and Windows; Docker Engine or Docker Desktop on Linux. Give Docker **at least 8 GB of memory**: each monthly file holds about 3M rows. |
| Git | |
| Python 3.10+ **or** [uv](https://docs.astral.sh/uv/) | Only for the local virtualenv (tests, notebook). The pipeline itself runs in containers. |
| `make`, `bash`, `curl` | Built into macOS and Linux; on Windows, use WSL 2 (below). |
| Free ports | 5433, 8081, 8088 (each can be changed in `.env`) |
| Disk | About 5 GB for the Docker images, plus about 100 MB per month of data (raw and cleaned) |

All platforms follow the same steps:

1. Clone the repository.
2. Create `.env` from `.env.example` and set the secrets.
3. Run `make setup`, which creates the venv, downloads the data and starts the containers.
4. Run `make unpause` to start the pipeline, then `make dashboard` once data is loaded.

### macOS

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it. In *Settings → Resources*, set the memory to 8 GB or more.
2. Install the command-line tools (`git`, `make`, `curl`): `xcode-select --install`.
   Optional: `brew install uv`.
3. Clone and configure:
   ```bash
   git clone <repository-url> nyc-taxi && cd nyc-taxi
   cp .env.example .env
   ```
   Edit `.env` and replace every `change-me` value ([generating secrets](#generating-secrets)).
4. Run:
   ```bash
   make setup
   make unpause
   ```

### Linux

1. Install [Docker Engine](https://docs.docker.com/engine/install/) with the Compose plugin, and let your user run Docker without `sudo`:
   ```bash
   sudo usermod -aG docker "$USER"    # then log out and back in
   docker compose version             # must print v2.x
   ```
2. Install the tools. On Debian or Ubuntu:
   ```bash
   sudo apt-get update && sudo apt-get install -y git make curl python3 python3-venv
   ```
3. Clone and configure:
   ```bash
   git clone <repository-url> nyc-taxi && cd nyc-taxi
   cp .env.example .env
   ```
   Edit `.env`: replace every `change-me` value, and **set `AIRFLOW_UID` to your user id** (`id -u`).
   Otherwise the Airflow containers cannot write to `logs/` and `data/`.
4. Run:
   ```bash
   make setup
   make unpause
   ```

### Windows

The scripts use `bash` and `make`, so the recommended setup is **WSL 2** (Windows Subsystem for Linux).

**Option A - WSL 2 (recommended)**

1. In PowerShell as Administrator, run `wsl --install` (installs Ubuntu) and reboot.
2. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/). In *Settings → General*, enable *Use the WSL 2 based engine*. In *Settings → Resources → WSL Integration*, enable your Ubuntu distribution.
3. Open the **Ubuntu** terminal and follow the [Linux](#linux) steps from step 2. Skip the Docker Engine install, because Docker Desktop provides `docker`.
4. Clone into the Linux file system (e.g. `~/nyc-taxi`), **not** under `/mnt/c/...`. Bind mounts from the Windows drive are slow and can break file permissions.

**Option B - PowerShell only (no WSL)**

You can run the containers directly, but `make` and `scripts/*.sh` are not available, so use the equivalent commands:

```powershell
git clone <repository-url> nyc-taxi; cd nyc-taxi
Copy-Item .env.example .env          # then edit .env and replace every change-me value

# Start Postgres, Airflow and Superset
docker compose up --detach --build --wait

# Start the pipeline; the extract task downloads the raw files itself
docker compose exec airflow-scheduler airflow dags unpause nyc_taxi_etl

# After the first months are loaded: build the dashboard
docker compose exec -T superset python /app/bootstrap/bootstrap_dashboard.py

# Optional: local virtualenv for tests and the notebook
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -r requirements\dev.txt
.venv\Scripts\python -m pytest
```

The repository's `.gitattributes` keeps shell scripts at LF line endings. If you cloned before it existed, re-clone, or run `git rm --cached -r . && git reset --hard`.
A script with Windows (CRLF) line endings fails inside the Postgres container.

### Generating secrets

Fill these in `.env` before the first start:

```bash
openssl rand -hex 32       # AIRFLOW_JWT_SECRET
openssl rand -base64 42    # SUPERSET_SECRET_KEY
```

Use strong values for `DW_PASSWORD` (also used as the Postgres superuser password), `AIRFLOW_DB_PASSWORD` and `SUPERSET_ADMIN_PASSWORD`.
Without OpenSSL (Windows PowerShell): `[Convert]::ToBase64String((1..42 | % {Get-Random -Max 256}))`.

Passwords only take effect when the database volume is first created. To change them later, run `docker compose down -v` (this deletes the data) and start again.

## Using the project

### Open the services

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8081 | none (every visitor is admin; local use only) |
| Superset dashboard | http://localhost:8088/superset/dashboard/nyc-taxi/ | `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` |
| PostgreSQL | `localhost:5433`, database `DW_DB` | `DW_USER` / `DW_PASSWORD` |

### Run the pipeline

1. `make unpause`: the scheduler catches up and loads each month between `DAG_START_DATE` and `DAG_END_DATE` (by default 2023-01 and 2023-02).
2. Follow progress in the Airflow UI (DAG `nyc_taxi_etl`), or with `make logs`.
3. When the runs succeed, run `make dashboard` to create the Superset database connection, datasets, charts and dashboard. It is safe to run again.

### Load more months

The DAG only runs inside `DAG_START_DATE`..`DAG_END_DATE`; Airflow skips runs outside that window, even manual ones.

1. Raise `DAG_END_DATE` in `.env` (e.g. `2023-06-30`).
2. `make restart` to recreate the containers with the new value.
3. Let catch-up run, or trigger the DAG in the Airflow UI with the parameter `month=YYYY-MM`.

Missing raw files are downloaded by the `extract` task. Re-running a month replaces it, so no duplicates are created.

### Make targets

| Command | What it does |
|---|---|
| `make setup` | Create `.venv`, install `requirements/dev.txt`, download data, start all services |
| `make up` / `make down` / `make restart` | Start / stop / recreate the services (`down` keeps the database volume) |
| `make ps` / `make logs` | Service status / follow container logs |
| `make unpause` | Unpause the `nyc_taxi_etl` DAG |
| `make dashboard` | Create or update the Superset dashboard |
| `make test` | Run the test suite |
| `make lint` | Check shell scripts (`bash -n`, and `shellcheck` if installed) |
| `make docs` | Regenerate `docs/schema/data_dictionary.csv` from `sql/schema.sql` |
| `make clean` | Remove Python caches |

`make help` lists them all. To delete everything, including the database: `docker compose down -v`.

### Develop and test

```bash
make test                          # all tests; the DAG integrity test is skipped where Airflow is not installed
scripts/run_tests.sh -k clean -q   # pass pytest arguments
```

- View and explore the raw files, the zone lookup and the cleaned output in [notebooks/main.ipynb](notebooks/main.ipynb), using the `.venv` kernel. Set `MONTH` in the first code cell to choose a month.
- CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) lints the scripts, runs the setup on Ubuntu, runs the tests, and checks that the databases were created.

## Configuration

All settings come from `.env` (gitignored); `.env.example` is the template and shows every default.
`docker-compose.yml` has no credential defaults and refuses to start if a required value is missing.

| Group | Variables | Default |
|---|---|---|
| Warehouse | `DW_USER`, `DW_PASSWORD`, `DW_DB`, `DW_PORT` | -, -, -, `5433` |
| Secrets | `AIRFLOW_DB_PASSWORD`, `AIRFLOW_JWT_SECRET`, `SUPERSET_SECRET_KEY` | required |
| Superset | `SUPERSET_PORT`, `SUPERSET_ADMIN_USER`, `SUPERSET_ADMIN_PASSWORD` | `8088`, -, - |
| Airflow | `AIRFLOW_PORT`, `AIRFLOW_UID` | `8081`, `50000` |
| Paths | `RAW_DIR`, `CLEAN_DIR`, `SQL_DIR` | `data/input`, `data/output`, `sql` |
| Cleaning rules | `MAX_TRIP_HOURS`, `MAX_TRIP_MILES`, `MAX_LOCATION_ID` | `24`, `200`, `265` |
| Quality checks | `MAX_UNKNOWN_PAYMENT_SHARE` | `0.10` |
| Schedule | `DAG_SCHEDULE`, `DAG_START_DATE`, `DAG_END_DATE`, `DAG_CATCHUP` | `@monthly`, `2023-01-01`, `2023-02-28`, `true` |
| Retries | `TASK_RETRIES`, `TASK_RETRY_DELAY_S`, `TASK_MAX_RETRY_DELAY_MIN`, `TASK_EXECUTION_TIMEOUT_MIN` | `2`, `30`, `5`, `30` |
| Alerts | `ALERT_WEBHOOK_URL` | empty (log only) |

The full list is in [src/config/settings.py](src/config/settings.py). Deployment details are in [docs/deployment.md](docs/deployment.md).

### Assumptions

- Only the **Yellow Taxi** dataset is used. A **month is the unit of load**, and pickups outside the month in a file are rejected.
- Revenue is `total_amount`. Average fare per mile is `sum(fare_amount) / sum(trip_distance)`.
- Airflow runs without login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`) and Superset uses an in-process cache. Both are fine locally; a shared deployment needs real authentication and Redis.

## Monitoring and logging

- **Audit table:** each step writes its status, start and end, duration, `rows_in`, `rows_out`, `rows_rejected`, rejects per rule and any error message to `dw.etl_run_audit`. The dashboard shows it.
  ```sql
  SELECT step, status, duration_s, rows_in, rows_out, rows_rejected
  FROM dw.etl_run_audit ORDER BY started_at DESC LIMIT 10;
  ```
- **Structured logs:** JSON lines with run id, month, row counts and errors, written to the Airflow task log and to `logs/etl/pipeline.jsonl` (`tail -f logs/etl/pipeline.jsonl | jq`). Airflow's own task logs are in `logs/dag_id=nyc_taxi_etl/`.
- **Error handling:** explicit exceptions (`SchemaError`, `DataQualityError`), retries with exponential backoff, and a timeout per task.
- **Alerts:** a failed task logs an ERROR alert and, if `ALERT_WEBHOOK_URL` is set, posts to a Slack-compatible webhook. Retries are logged.

### Dashboard metrics

| Metric | SQL |
|---|---|
| KPI tiles (trips, revenue, average fare per mile, average distance) | [sql/reporting/kpi_summary.sql](sql/reporting/kpi_summary.sql) |
| Average fare per mile (overall, by month, by pickup borough) | `sql/reporting/avg_fare_per_mile*.sql` |
| Peak hours for rides | [sql/reporting/peak_hours.sql](sql/reporting/peak_hours.sql) |
| Total revenue by payment type | [sql/reporting/revenue_by_payment_type.sql](sql/reporting/revenue_by_payment_type.sql) |
| Pipeline run history | [sql/reporting/pipeline_runs.sql](sql/reporting/pipeline_runs.sql) |

## Project structure

```
├── Makefile                     common tasks (make help)
├── .env.example                 configuration template, copied to .env
├── .dockerignore / .gitignore / .gitattributes
├── data/
│   ├── input/                   raw TLC parquet files and zone lookup (downloaded)
│   └── output/                  cleaned parquet files, one per month
├── Docker/
│   ├── Dockerfile_airflow       Airflow image + requirements/prod.txt
│   └── Dockerfile_superset      Superset image + PostgreSQL driver
├── docker-compose.yml           Postgres, Airflow, Superset
├── dags/nyc_taxi_etl.py         Airflow DAG
├── docs/                        deployment, ETL flow, business rules, ER diagram, data dictionary
├── logs/                        Airflow task logs and logs/etl/pipeline.jsonl (runtime)
├── notebooks/main.ipynb         view and explore raw and cleaned data
├── requirements/                base.txt (runtime), dev.txt (+ pytest, ipykernel), prod.txt (Airflow image)
├── scripts/
│   ├── setup.sh                 one-shot setup
│   ├── run_tests.sh             run the test suite
│   ├── init-databases.sh        Postgres first-start hook: creates the databases
│   └── generate_data_dictionary.py
├── sql/
│   ├── schema.sql               star schema DDL
│   ├── seed_dimensions.sql      static dimension rows
│   ├── queries/                 dimension and fact load SQL
│   └── reporting/               dashboard queries
├── src/
│   ├── config/                  settings.py (all settings), logging_config.py (JSON logging)
│   ├── db_connection/           postgres.py (connections), reader.py (run SQL files)
│   ├── extract/fetch_data.py    download and validate raw files
│   ├── transform/clean_trips.py cleaning rules with per-rule reject counts
│   ├── load/load_to_db.py       COPY into staging, dimension and fact loads
│   ├── quality/checks.py        post-load data quality checks
│   ├── schema/trips.py          column contract shared with the DDL
│   └── utils/                   audit.py (dw.etl_run_audit), alerts.py (failure webhook)
├── superset/                    Superset config and dashboard-as-code bootstrap
└── tests/                       cleaning rules, DAG integrity, schema consistency
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `docker compose` says `set X in .env` | A required variable is missing from `.env`; compare it with `.env.example`. |
| Port already in use | Change `DW_PORT`, `AIRFLOW_PORT` or `SUPERSET_PORT` in `.env`, then run `make restart`. |
| Postgres login fails after changing a password | Passwords apply only to a new volume: `docker compose down -v`, then `make up` (this deletes the data). |
| Linux: `Permission denied` under `logs/` or `data/` | Set `AIRFLOW_UID=$(id -u)` in `.env`, then run `make restart`. |
| Windows: `init-databases.sh` fails with `\r: command not found` | The script has CRLF line endings; see the note in [Windows](#windows). |
| A task is killed or the containers restart during `clean` | Give Docker more memory (8 GB or more). |
| A manual run is skipped | Its month is outside `DAG_START_DATE`..`DAG_END_DATE`; see [Load more months](#load-more-months). |
| The dashboard is empty | Run `make dashboard` after at least one successful DAG run. |
| `make: command not found` (Windows) | Use WSL 2, or the PowerShell commands in [Windows Option B](#windows). |

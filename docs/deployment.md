# Deployment and configuration

Everything runs locally with Docker Compose: one PostgreSQL 16 instance (Airflow metadata, Superset metadata and the warehouse), Airflow 3 (LocalExecutor) and Superset 6.1.

## Prerequisites

- Docker with Compose v2
- `curl`
- `uv`, or Python 3.10+

## Configuration

All settings come from `.env` (gitignored). `scripts/setup.sh` creates it from `.env.example` if it is missing.

**Before the first start**, set real values for:

| Variable | Used by |
|---|---|
| `DW_USER`, `DW_PASSWORD`, `DW_DB` | Warehouse database and role |
| `AIRFLOW_DB_PASSWORD` | Airflow and Superset metadata databases |
| `AIRFLOW_JWT_SECRET` | Airflow API server |
| `SUPERSET_SECRET_KEY` | Superset (e.g. `openssl rand -base64 42`) |
| `SUPERSET_ADMIN_USER`, `SUPERSET_ADMIN_PASSWORD` | Superset login |

`docker-compose.yml` has no credential defaults and refuses to start if one of these is missing.

Optional settings (paths, cleaning thresholds, retries, DAG window, alert webhook) default to the values in `src/config/settings.py`; each can be overridden in `.env` with the variable of the same name.

## Start

```bash
make setup        # venv, dependencies, data download, docker compose up
make unpause      # the scheduler runs the months inside the DAG window
make dashboard    # create the Superset dashboard (idempotent)
```

`make help` lists all targets.

| Service | URL |
|---|---|
| Airflow | http://localhost:8081 (`AIRFLOW_PORT`) |
| Superset | http://localhost:8088/superset/dashboard/nyc-taxi/ (`SUPERSET_PORT`) |
| PostgreSQL | localhost:5433 (`DW_PORT`) |

## Containers and images

| File | Purpose |
|---|---|
| `Docker/Dockerfile_airflow` | `apache/airflow` plus `requirements/prod.txt` |
| `Docker/Dockerfile_superset` | `apache/superset` plus the PostgreSQL driver |
| `scripts/init-databases.sh` | Postgres first-start hook: creates the warehouse, Airflow and Superset databases |

The Airflow containers mount the repository at `/opt/airflow/project` (on `PYTHONPATH`, so `src` is importable), `dags/` at `/opt/airflow/dags` and `logs/` at `/opt/airflow/logs`. Superset mounts `superset/` and `sql/` read-only.

## Dependencies

| File | Installed into |
|---|---|
| `requirements/base.txt` | Core runtime (pandas, pyarrow, psycopg, python-dotenv) |
| `requirements/dev.txt` | Local `.venv`: base + pytest + ipykernel |
| `requirements/prod.txt` | Airflow image |

## Loading more months

The DAG only runs inside `DAG_START_DATE`..`DAG_END_DATE` (defaults `2023-01-01`..`2023-02-28`). Airflow skips runs outside that window, even manual ones. To load more:

1. Raise `DAG_END_DATE` in `.env`.
2. Recreate the Airflow containers: `make restart`.
3. Trigger a run with `month=YYYY-MM`, or backfill.

The month's file must exist on the TLC site. If it is not in `data/input/`, the `extract` task downloads it.

## Reset

```bash
make down
docker compose down -v    # also deletes the database volume
```

# Technical documentation

The project overview and quick start are in the [root README](../README.md). This folder holds the detail.

| Document | What it covers |
|---|---|
| [deployment.md](deployment.md) | Prerequisites, configuration (`.env`), starting the stack, loading more months, resetting |
| [etl/pipeline_flow.md](etl/pipeline_flow.md) | The Airflow DAG: task order, schedule, retries, idempotency, monitoring |
| [etl/business_rules.md](etl/business_rules.md) | Cleaning rules, post-load quality checks, metric definitions |
| [schema/er_diagram.md](schema/er_diagram.md) | Star schema entity-relationship diagram |
| [schema/data_dictionary.csv](schema/data_dictionary.csv) | Every table column with type, nullability and description |

`schema/data_dictionary.csv` is generated from `sql/schema.sql`. Regenerate it after changing the DDL:

```bash
make docs
```

## Where the code lives

| Path | Responsibility |
|---|---|
| `dags/nyc_taxi_etl.py` | Airflow DAG: wires the steps together, retries, alerts |
| `src/config/settings.py` | Every setting, overridable from `.env` |
| `src/config/logging_config.py` | JSON structured logging |
| `src/db_connection/` | PostgreSQL connections and SQL file execution |
| `src/extract/fetch_data.py` | Download and validate the raw TLC files |
| `src/transform/clean_trips.py` | Cleaning rules with per-rule reject counts |
| `src/load/load_to_db.py` | Bulk `COPY` into staging, dimension and fact loads |
| `src/quality/checks.py` | Post-load data quality checks |
| `src/schema/trips.py` | Column contract shared by the Python code and the DDL |
| `src/utils/` | Audit trail (`dw.etl_run_audit`) and failure alerts |
| `sql/schema.sql`, `sql/seed_dimensions.sql` | DDL and static dimension rows |
| `sql/queries/` | Dimension and fact load SQL |
| `sql/reporting/` | Dashboard queries (Superset datasets) |

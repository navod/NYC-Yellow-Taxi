#!/bin/bash
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE "${DW_USER}" LOGIN PASSWORD '${DW_PASSWORD}';
    CREATE DATABASE "${DW_DB}" OWNER "${DW_USER}";
    CREATE DATABASE superset;
    CREATE ROLE airflow LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE airflow OWNER airflow;
EOSQL

#!/usr/bin/env bash
# One-shot local setup for the NYC Taxi batch ETL project:
#   create a Python virtual environment and install dependencies

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

MONTHS="${MONTHS:-2023-01 2023-02}"
BASE_URL="https://d37ci6vzurychx.cloudfront.net"
RAW_DIR="data/input"
CLEAN_DIR="data/output"
REQUIREMENTS="requirements/dev.txt"
VENV_DIR=".venv"

log()  { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }

trap 'fail "scripts/setup.sh failed at line $LINENO"' ERR

# ------------------------------------------------------------------ prerequisites
check_prerequisites() {
    [[ -f "$REQUIREMENTS" ]] || fail "$REQUIREMENTS not found in $(pwd)"

    # uv brings its own interpreter; otherwise we need a system Python 3.10+.
    if ! command -v uv >/dev/null; then
        command -v python3 >/dev/null || fail "python3 (3.10+) is required"
        python3 -c 'import sys; sys.exit(sys.version_info < (3, 10))' \
            || fail "python3 3.10+ is required (found $(python3 --version))"
    fi
}

# ------------------------------------------------------------------ python venv
create_venv() {
    if [[ -x "$VENV_DIR/bin/python" ]]; then
        log "Virtual environment already exists at $VENV_DIR"
    elif command -v uv >/dev/null; then
        # uv can fetch a pinned interpreter, independent of the system Python.
        log "Creating virtual environment with uv (Python 3.12)"
        uv venv --python 3.12 "$VENV_DIR"
    else
        log "Creating virtual environment with $(python3 --version)"
        python3 -m venv "$VENV_DIR"
    fi

    log "Installing Python dependencies"
    if command -v uv >/dev/null; then
        uv pip install --quiet --python "$VENV_DIR/bin/python" -r "$REQUIREMENTS"
    else
        "$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip
        "$VENV_DIR/bin/python" -m pip install --quiet -r "$REQUIREMENTS"
    fi
}

download() {
    local url="$1" dest="$2"
    if [[ -s "$dest" ]]; then
        log "Already downloaded: $dest"
        return
    fi
    log "Downloading $url"
    curl --fail --location --silent --show-error \
         --retry 3 --retry-delay 5 \
         --output "$dest.part" "$url"
    mv "$dest.part" "$dest"
    log "Saved $dest ($(du -h "$dest" | cut -f1))"
}

download_data() {
    mkdir -p "$RAW_DIR" "$CLEAN_DIR" logs
    download "$BASE_URL/misc/taxi_zone_lookup.csv" "$RAW_DIR/taxi_zone_lookup.csv"
    for month in $MONTHS; do
        [[ "$month" =~ ^[0-9]{4}-[0-9]{2}$ ]] || fail "Invalid month '$month' (expected YYYY-MM)"
        download "$BASE_URL/trip-data/yellow_tripdata_${month}.parquet" \
                 "$RAW_DIR/yellow_tripdata_${month}.parquet"
    done
}

start_services() {
    if [[ ! -f .env ]]; then
        cp .env.example .env
        log "Created .env from .env.example"
    fi
    log "Building and starting PostgreSQL and Airflow (first run pulls images)"
    docker compose up --detach --build --wait
}

main() {
    log "Setup started"
    check_prerequisites
    create_venv
    download_data
    start_services
    log "Setup finished"
    cat <<EOF

Next steps:
  source $VENV_DIR/bin/activate
  Airflow UI   : http://localhost:${AIRFLOW_PORT:-8081}   (unpause the 'nyc_taxi_etl' DAG)
  Run pipeline : docker compose exec airflow-scheduler airflow dags unpause nyc_taxi_etl
  Superset     : http://localhost:${SUPERSET_PORT:-8088}/superset/dashboard/nyc-taxi/   (login user and password from SUPERSET_ADMIN_USER / SUPERSET_ADMIN_PASSWORD in .env)

  Tests        : make test
EOF
}

main "$@"

#!/usr/bin/env bash
# One-shot local setup for the NYC Taxi batch ETL project:
#   create a Python virtual environment and install dependencies

set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

VENV_DIR=".venv"

log()  { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
fail() { log "ERROR: $*" >&2; exit 1; }

trap 'fail "setup.sh failed at line $LINENO"' ERR

# ------------------------------------------------------------------ prerequisites
check_prerequisites() {
    [[ -f requirements.txt ]] || fail "requirements.txt not found in $(pwd)"

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
        uv pip install --quiet --python "$VENV_DIR/bin/python" -r requirements.txt
    else
        "$VENV_DIR/bin/python" -m pip install --quiet --upgrade pip
        "$VENV_DIR/bin/python" -m pip install --quiet -r requirements.txt
    fi
}

main() {
    log "Setup started"
    check_prerequisites
    create_venv

    log "Setup finished"
    cat <<EOF

Next steps:
  source $VENV_DIR/bin/activate
EOF
}

main "$@"

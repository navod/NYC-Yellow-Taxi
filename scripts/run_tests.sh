#!/usr/bin/env bash
# Run the automated test suite with the project virtualenv.
# Extra arguments are passed to pytest, e.g. scripts/run_tests.sh -k clean -q
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

[[ -x .venv/bin/python ]] || { echo "No .venv found; run scripts/setup.sh first" >&2; exit 1; }
exec .venv/bin/python -m pytest "$@"

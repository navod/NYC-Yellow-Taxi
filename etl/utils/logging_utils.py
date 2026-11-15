"""Structured (JSON) logging for the pipeline.

Every log record becomes one JSON object per line, so logs can be searched and
aggregated by field (step, month, rows_out ...) instead of parsed as free text.

Usage::

    logger = get_logger(__name__)
    logger.info("step finished", extra={"step": "clean", "rows_out": 2_900_000})
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from etl import config

# Attributes every LogRecord has. Anything else on a record was passed by the
# caller through ``extra=`` and belongs in the JSON output.
_STANDARD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}

_ROOT_LOGGER_NAME = "etl"


class JsonFormatter(logging.Formatter):
    """Format a log record as a single-line JSON document."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Structured fields passed via ``extra=``.
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # default=str keeps logging from ever failing on dates, Paths, Decimals ...
        return json.dumps(payload, default=str)


def _configure_root_logger() -> logging.Logger:
    """Attach handlers to the package-level logger exactly once."""
    root = logging.getLogger(_ROOT_LOGGER_NAME)
    if root.handlers:
        return root

    root.setLevel(config.LOG_LEVEL)
    formatter = JsonFormatter()

    # stdout: captured by Airflow and shown in the task log of the UI.
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Optional JSON-lines file, handy for `tail -f logs/etl/pipeline.jsonl | jq`.
    log_dir = config.LOG_DIR
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(Path(log_dir) / "pipeline.jsonl")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Our handlers already cover the output; do not duplicate through the root logger.
    root.propagate = False
    return root


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the ``etl`` namespace with JSON handlers configured."""
    _configure_root_logger()
    if name == _ROOT_LOGGER_NAME or name.startswith(f"{_ROOT_LOGGER_NAME}."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT_LOGGER_NAME}.{name}")

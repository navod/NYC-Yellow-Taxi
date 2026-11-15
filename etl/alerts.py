"""Failure alerting: always a structured ERROR log, plus an optional webhook."""

from __future__ import annotations

import json
import urllib.request
from typing import Any

from etl import config
from etl.utils.logging_utils import get_logger

logger = get_logger(__name__)


def send_failure_alert(details: dict[str, Any]) -> None:
    """Emit an alert for a failed task.

    The ERROR log line is the baseline alert channel (any log shipper can
    trigger on ``"alert": true``). If ALERT_WEBHOOK_URL is set, the same
    message is also posted to a Slack-compatible incoming webhook.
    """
    logger.error("ALERT: pipeline task failed", extra={"alert": True, **details})

    webhook_url = config.ALERT_WEBHOOK_URL
    if not webhook_url:
        return

    text = "NYC taxi ETL failure\n" + "\n".join(f"- {k}: {v}" for k, v in details.items())
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=config.ALERT_TIMEOUT_S).close()
    except OSError:
        logger.warning("could not deliver webhook alert", exc_info=True)

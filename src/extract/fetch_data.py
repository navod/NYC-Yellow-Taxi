from __future__ import annotations

import shutil
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

from src.config import settings as config
from src.config.logging_config import get_logger

logger = get_logger(__name__)



class ExtractError(Exception):
    """The raw file is missing upstream or is not a valid parquet file."""


def _download(url: str, dest: Path) -> None:
    """Stream ``url`` to ``dest`` through a temporary file (no partial files left behind)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".part")
    logger.info("downloading file", extra={"url": url, "dest": dest})
    try:
        with urllib.request.urlopen(url, timeout=config.DOWNLOAD_TIMEOUT_S) as response:
            with partial.open("wb") as out:
                shutil.copyfileobj(response, out)
    except OSError as exc:  # URLError / HTTPError / timeouts are all OSError subclasses
        partial.unlink(missing_ok=True)
        raise ExtractError(f"Download failed for {url}: {exc}") from exc
    partial.replace(dest)


def ensure_zone_lookup() -> Path:
    """Return the path of the taxi zone lookup CSV, downloading it if needed."""
    if not config.ZONE_LOOKUP_PATH.exists():
        _download(config.ZONE_LOOKUP_URL, config.ZONE_LOOKUP_PATH)
    return config.ZONE_LOOKUP_PATH


def ensure_raw_trips(month: str) -> tuple[Path, int]:
    """Return (path, row count) of the raw trip file for ``month``.

    scripts/setup.sh normally downloads the file already; the pipeline downloads it
    itself when it is missing, so a new month needs no manual step.
    """
    path = config.raw_trip_path(month)
    if not path.exists():
        _download(config.raw_trip_url(month), path)

    # Reading the parquet footer is cheap and proves the file is complete.
    try:
        row_count = pq.ParquetFile(path).metadata.num_rows
    except Exception as exc:
        raise ExtractError(f"{path} is not a readable parquet file: {exc}") from exc

    if row_count == 0:
        raise ExtractError(f"{path} contains no rows")
    return path, row_count

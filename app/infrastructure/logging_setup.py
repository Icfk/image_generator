"""Consistent, idempotent logging setup for each service process."""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from app.infrastructure.config import settings


def configure_logging(process: str) -> None:
    """Keep seven daily log files in the configured directory per process."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    # HTTPX INFO includes full URLs, whose query strings may contain secrets.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    directory = Path(settings.log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    target = str((directory / f"{process}.log").resolve())
    root = logging.getLogger()
    if any(
        getattr(handler, "baseFilename", None) == target for handler in root.handlers
    ):
        return
    handler = TimedRotatingFileHandler(
        target, when="midnight", backupCount=7, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(handler)

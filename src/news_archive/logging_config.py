"""Structured JSON logging setup.

JSON logs survive `tail -f` and `grep` on the droplet and are easy to ship to a
log aggregator later. Call `configure_logging()` once per process entrypoint.
"""

from __future__ import annotations

import logging
import sys

import structlog

from news_archive.config import settings


def configure_logging(level: str | None = None) -> None:
    log_level = (level or settings.log_level).upper()

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(log_level),
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)

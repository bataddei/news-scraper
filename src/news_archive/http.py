"""Shared HTTP helpers used by every collector.

Every outbound request:
  * carries the operator's User-Agent (SEC EDGAR rejects requests without one;
    politeness convention for scraping elsewhere),
  * has a 30-second timeout so a wedged upstream never hangs a collector,
  * retries 3 times with exponential backoff on network errors and 5xx.

`fetch_bytes` returns the raw body so callers can hand it to feedparser, a
JSON parser, or an HTML parser without extra decoding.
"""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from news_archive.config import settings
from news_archive.logging_config import get_logger

log = get_logger(__name__)

DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _default_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "*/*",
    }
    if extra:
        headers.update(extra)
    return headers


class RetryableHTTPError(Exception):
    """Wraps a retryable response (5xx or 429) so tenacity retries it."""


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.TransportError, RetryableHTTPError)),
    reraise=True,
)
def fetch_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> bytes:
    """GET `url` and return raw bytes. Retries on 429 + 5xx + network errors; raises on other 4xx."""
    with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
        response = client.get(url, headers=_default_headers(headers), params=params)
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise RetryableHTTPError(f"{url} returned {response.status_code}")
        response.raise_for_status()
        return response.content


def fetch_text(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> str:
    """GET `url` and return the response body decoded as text."""
    raw = fetch_bytes(url, headers=headers, params=params)
    return raw.decode("utf-8", errors="replace")

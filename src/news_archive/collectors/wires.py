"""Press-wire RSS collectors — one collector class per wire.

Three wire services are declared as Tier 2 sources in the brief:

    * PR Newswire
    * GlobeNewswire
    * Business Wire

They're all RSS, and the per-entry shape is similar enough that we share one
base class (`RssWireCollector`) and subclass it with the feed URL + org name.
The important divergence is ticker extraction:

    * GlobeNewswire tags entries with `NYSE:TICKER` / `Nasdaq:TICKER` — parseable.
    * PR Newswire tags with 3-letter industry/subject codes (AWD, PER, HSP, ...);
      no reliable ticker hint in the feed itself.
    * Business Wire blocks public RSS behind edge WAF (403s even with a proper
      User-Agent) — class is defined for completeness and future cookie-auth
      work, but the class is not currently registered in the cron dispatcher.

Per the brief's "archive is the product" principle, we ingest every entry and
tag tickers when the feed surfaces them. No Mag 7 pre-filter at ingest —
filtering at query time preserves optionality.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any, ClassVar

import feedparser

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

# Recognised exchange prefixes for ticker extraction from GlobeNewswire tags.
_EXCHANGES = (
    "NYSE", "NASDAQ", "NYSEMKT", "NYSE American", "AMEX", "OTC", "OTCQX",
    "OTCQB", "TSX", "TSXV", "LSE", "ASX", "HKEX", "BSE", "NSE", "BIST",
    "Nasdaq",
)
_EXCHANGE_UPPER = {e.upper() for e in _EXCHANGES}

_TICKER_TAG_RE = re.compile(r"^\s*([A-Za-z][A-Za-z ]{0,19}):([A-Za-z][A-Za-z0-9.\-]{0,9})\s*$")


def _parsed_time_to_utc(st: struct_time | None) -> datetime | None:
    if st is None:
        return None
    try:
        return datetime(
            st.tm_year, st.tm_mon, st.tm_mday,
            st.tm_hour, st.tm_min, st.tm_sec,
            tzinfo=UTC,
        )
    except (TypeError, ValueError):
        return None


def extract_tickers_from_tags(tags: Iterable[Any] | None) -> list[str]:
    """Return ticker symbols parsed from RSS <category> tags of the form EXCHANGE:TICKER.

    Only accepts tags whose exchange prefix is in the known-exchanges allowlist.
    Deduplicates while preserving feed order. Returns symbols upper-cased.
    """
    if not tags:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for t in tags:
        term = (t.get("term") or "").strip() if isinstance(t, dict) else None
        if not term:
            continue
        m = _TICKER_TAG_RE.match(term)
        if not m:
            continue
        exchange = m.group(1).strip().upper()
        if exchange not in _EXCHANGE_UPPER:
            continue
        symbol = m.group(2).upper()
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _entry_to_dict(entry: Any, *, feed_url: str, wire: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "_source_feed": feed_url,
        "_wire": wire,
    }
    for key in (
        "title", "link", "id", "guid", "summary", "description",
        "author", "published", "updated", "publisher",
    ):
        val = entry.get(key)
        if val is not None:
            out[key] = val
    tags = entry.get("tags")
    if tags:
        out["tags"] = [
            {"term": t.get("term"), "label": t.get("label")}
            for t in tags
            if t.get("term")
        ]
    return out


class RssWireCollector(BaseCollector):
    """Shared logic for an RSS-backed press-wire source."""

    # Subclasses must set these.
    feed_url: ClassVar[str] = ""
    wire_name: ClassVar[str] = ""

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        raw = http.fetch_bytes(self.feed_url)
        fetched_at = utcnow()

        feed = feedparser.parse(raw)
        if feed.bozo:
            self.logger.warning(
                "feed.parse_warning",
                feed_url=self.feed_url,
                bozo_reason=str(feed.bozo_exception),
            )

        self.logger.info(
            "feed.loaded",
            feed_url=self.feed_url,
            wire=self.wire_name,
            entries=len(feed.entries),
        )

        for entry in feed.entries:
            published = _parsed_time_to_utc(entry.get("published_parsed"))
            if published is None:
                self.logger.warning(
                    "item.skipped_no_pubdate",
                    wire=self.wire_name,
                    title=entry.get("title", "")[:120],
                )
                continue

            headline = (entry.get("title") or "").strip()
            if not headline:
                self.logger.warning(
                    "item.skipped_no_headline",
                    wire=self.wire_name,
                    link=entry.get("link"),
                )
                continue

            url = entry.get("link") or None
            body = entry.get("summary") or entry.get("description") or None
            external_id = entry.get("id") or entry.get("guid") or url
            raw_payload = _entry_to_dict(entry, feed_url=self.feed_url, wire=self.wire_name)

            article = Article(
                source_id=self.source_id,
                external_id=external_id,
                url=url,
                headline=headline,
                body=body,
                source_published_at=published,
                source_fetched_at=fetched_at,
                raw_payload=raw_payload,
                content_hash=content_hash(headline, body),
                language="en",
            )
            entities: list[ArticleEntity] = [
                ArticleEntity(entity_type="event", entity_value="PressRelease"),
                ArticleEntity(entity_type="org", entity_value=self.wire_name),
            ]
            for symbol in extract_tickers_from_tags(entry.get("tags")):
                entities.append(ArticleEntity(entity_type="ticker", entity_value=symbol))
            yield article, entities


class PRNewswireCollector(RssWireCollector):
    source_slug = "wire_pr_newswire"
    feed_url = "https://www.prnewswire.com/rss/news-releases-list.rss"
    wire_name = "PR Newswire"


class GlobeNewswireCollector(RssWireCollector):
    source_slug = "wire_globenewswire"
    feed_url = (
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/"
        "GlobeNewswire%20-%20News%20about%20Public%20Companies"
    )
    wire_name = "GlobeNewswire"


class BusinessWireCollector(RssWireCollector):
    """Defined but NOT registered — Business Wire's public feed returns 403 to
    non-browser clients. Kept here so re-enabling is a one-line dispatcher
    change once we have a viable access path (paid feed, cookie-auth, etc.).
    """

    source_slug = "wire_business_wire"
    feed_url = "https://www.businesswire.com/portal/site/home/news/"
    wire_name = "Business Wire"

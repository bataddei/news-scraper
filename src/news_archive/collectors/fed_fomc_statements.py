"""FOMC statements (and minutes) collector — Federal Reserve monetary policy RSS.

Source choice:
    The brief names this source as a "scraper" of the FOMC calendar page. We use
    the Fed's own RSS feed (`press_monetary.xml`) instead because it gives an
    authoritative `<pubDate>` for `source_published_at` — which is CRITICAL per
    the brief's timestamp protocol — and covers the same set of statements and
    minutes that the calendar page links to. Scraping the calendar page HTML
    can be added later as a backfill or augmentation without changing the schema.

What lands in the archive:
    * FOMC policy statements (released ~2pm ET on decision days)
    * FOMC minutes (released 3 weeks after each meeting)
    * Implementation notes and related monetary-policy press releases

Dedup:
    * `external_id` = the RSS item's `guid` (the Fed publishes stable guids)
    * `content_hash` = SHA-256 of normalized (headline + body)

Entities tagged at ingest:
    * (event, FOMC) on every item
    * (org,   Federal Reserve)
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any

import feedparser

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

FEED_URL = "https://www.federalreserve.gov/feeds/press_monetary.xml"


def _parsed_time_to_utc(st: struct_time | None) -> datetime | None:
    """feedparser gives struct_time in UTC; convert to a tz-aware datetime."""
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


class FOMCStatementsCollector(BaseCollector):
    source_slug = "fed_fomc_statements"
    feed_url = FEED_URL

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        raw = http.fetch_bytes(self.feed_url)
        fetched_at = utcnow()

        feed = feedparser.parse(raw)
        if feed.bozo:
            self.logger.warning(
                "feed.parse_warning",
                bozo_reason=str(feed.bozo_exception),
                feed_url=self.feed_url,
            )

        self.logger.info("feed.loaded", entries=len(feed.entries), feed_url=self.feed_url)

        for entry in feed.entries:
            published = _parsed_time_to_utc(entry.get("published_parsed"))
            if published is None:
                # Timestamp protocol is non-negotiable. Skip rather than fabricate.
                self.logger.warning(
                    "item.skipped_no_pubdate",
                    title=entry.get("title", "")[:120],
                    link=entry.get("link"),
                )
                continue

            headline = (entry.get("title") or "").strip()
            if not headline:
                self.logger.warning("item.skipped_no_headline", link=entry.get("link"))
                continue

            url = entry.get("link") or None
            body = entry.get("summary") or entry.get("description") or None
            external_id = entry.get("id") or entry.get("guid") or None
            raw_payload = _entry_to_dict(entry)

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
            entities = [
                ArticleEntity(entity_type="event", entity_value="FOMC"),
                ArticleEntity(entity_type="org", entity_value="Federal Reserve"),
            ]
            yield article, entities


def _entry_to_dict(entry: Any) -> dict[str, Any]:
    """feedparser entries are dict-like but not JSON-serializable. Flatten safely."""
    out: dict[str, Any] = {}
    for key in (
        "title", "link", "id", "guid", "summary", "description",
        "author", "published", "updated",
    ):
        val = entry.get(key)
        if val is not None:
            out[key] = val
    tags = entry.get("tags")
    if tags:
        out["tags"] = [t.get("term") for t in tags if t.get("term")]
    return out

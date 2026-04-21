"""BLS economic releases collector — 5 per-indicator RSS feeds.

BLS publishes a dedicated RSS feed per indicator. We poll all of them in one
collector and tag each article with the indicator that produced it
(release_type entity). The per-feed `<pubDate>` is authoritative and gives us
the exact release timestamp (BLS embargoes break at 8:30 ET), which is
precisely the kind of timestamp fidelity the brief's timestamp protocol
depends on.

Fail-safe design:
    Each feed fetch is wrapped in its own try/except. If BLS returns 5xx on
    one feed we log a warning and continue with the others — the run still
    produces value, and BaseCollector will mark it `partial` via the
    articles_failed path only if individual article inserts fail. We also
    log feed-level failures so Week 4 integrity reporting can surface them.

Release types covered:
    * CPI                 — Consumer Price Index
    * PPI                 — Producer Price Index
    * EmploymentSituation — monthly jobs report (includes NFP)
    * JOLTS               — Job Openings and Labor Turnover Survey
    * RealEarnings        — real average hourly earnings
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

FEEDS: dict[str, str] = {
    "https://www.bls.gov/feed/cpi.rss": "CPI",
    "https://www.bls.gov/feed/ppi.rss": "PPI",
    "https://www.bls.gov/feed/empsit.rss": "EmploymentSituation",
    "https://www.bls.gov/feed/jolts.rss": "JOLTS",
    "https://www.bls.gov/feed/realer.rss": "RealEarnings",
}


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


def _entry_to_dict(entry: Any, *, feed_url: str, release_type: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "_source_feed": feed_url,
        "_release_type": release_type,
    }
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


class BLSReleasesCollector(BaseCollector):
    source_slug = "bls_releases"
    feeds = FEEDS

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        for feed_url, release_type in self.feeds.items():
            try:
                yield from self._collect_one_feed(feed_url, release_type)
            except Exception as e:
                # Fail one feed, not the whole run. Week 4 reporting will see this
                # because the other feeds' inserts still succeed but this feed
                # produced 0 — combined with the warning, that's a detectable gap.
                self.logger.warning(
                    "feed.failed",
                    feed_url=feed_url,
                    release_type=release_type,
                    error=str(e),
                    error_type=type(e).__name__,
                )

    def _collect_one_feed(
        self, feed_url: str, release_type: str,
    ) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        raw = http.fetch_bytes(feed_url)
        fetched_at = utcnow()

        feed = feedparser.parse(raw)
        if feed.bozo:
            self.logger.warning(
                "feed.parse_warning",
                feed_url=feed_url,
                bozo_reason=str(feed.bozo_exception),
            )

        self.logger.info(
            "feed.loaded",
            feed_url=feed_url,
            release_type=release_type,
            entries=len(feed.entries),
        )

        for entry in feed.entries:
            published = _parsed_time_to_utc(entry.get("published_parsed"))
            if published is None:
                self.logger.warning(
                    "item.skipped_no_pubdate",
                    feed_url=feed_url,
                    title=entry.get("title", "")[:120],
                )
                continue

            headline = (entry.get("title") or "").strip()
            if not headline:
                self.logger.warning(
                    "item.skipped_no_headline",
                    feed_url=feed_url,
                    link=entry.get("link"),
                )
                continue

            url = entry.get("link") or None
            body = entry.get("summary") or entry.get("description") or None
            external_id = entry.get("id") or entry.get("guid") or None
            raw_payload = _entry_to_dict(entry, feed_url=feed_url, release_type=release_type)

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
                ArticleEntity(entity_type="release_type", entity_value=release_type),
                ArticleEntity(entity_type="org", entity_value="BLS"),
            ]
            yield article, entities

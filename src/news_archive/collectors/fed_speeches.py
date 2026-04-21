"""Fed speeches and testimony collector — Federal Reserve RSS.

Source:
    https://www.federalreserve.gov/feeds/speeches_and_testimony.xml
    covers governors, regional presidents, and congressional testimony.

Speaker extraction:
    The RSS `<author>` field is empty. The title format is rigid though —
    "<LastName>, <Topic>" — and the URL slug encodes the same last name
    (e.g. `/speech/waller20260417a.htm`). We parse the last name from the
    URL (more stable than the title, which can contain commas in the topic)
    and store it as a `person` entity. A follow-up NLP backfill can add
    full names and affiliations later without changing the schema.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any

import feedparser

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

FEED_URL = "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"

# /newsevents/speech/<lastname><YYYYMMDD><letter>.htm
# /newsevents/testimony/<lastname><YYYYMMDD><letter>.htm
_SPEAKER_URL_RE = re.compile(r"/(?:speech|testimony)/([a-z]+)\d{8}", re.IGNORECASE)


def extract_speaker_last_name(url: str | None) -> str | None:
    """Parse the speaker's last name from a Fed speech/testimony URL. None if unparseable."""
    if not url:
        return None
    m = _SPEAKER_URL_RE.search(url)
    if m is None:
        return None
    return m.group(1).capitalize()


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


def _entry_to_dict(entry: Any) -> dict[str, Any]:
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


def _is_testimony(url: str | None) -> bool:
    return bool(url and "/testimony/" in url)


class FedSpeechesCollector(BaseCollector):
    source_slug = "fed_speeches"
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

            entity_kind = "FedTestimony" if _is_testimony(url) else "FedSpeech"
            entities = [
                ArticleEntity(entity_type="event", entity_value=entity_kind),
                ArticleEntity(entity_type="org", entity_value="Federal Reserve"),
            ]
            speaker = extract_speaker_last_name(url)
            if speaker:
                entities.append(ArticleEntity(entity_type="person", entity_value=speaker))
            else:
                # Speaker ID is useful signal — log when we can't extract one so we can
                # improve the regex rather than silently lose the entity.
                self.logger.warning("speaker.extract_failed", url=url, title=headline[:120])

            yield article, entities

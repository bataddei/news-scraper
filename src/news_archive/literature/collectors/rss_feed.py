"""Generic RSS/Atom feed collector, parameterized by source slug.

One class, N instances — one per active source row in `literature.sources`
whose `source_type` is 'rss' (currently blogs and journals). Adding a new
feed is a migration (insert a row with slug + feed_url); no code change
required. That constraint is why this class is parameterized by slug
rather than following the news-side pattern of one subclass per feed.

Dedup:
    * external_id = feed entry GUID / id / link (best available).
    * content_hash = SHA-256 of normalized (title + abstract). Abstract is the
      RSS summary/description; full-content scraping is a later-stage concern.

Author handling:
    feedparser normalizes to `entry.author` (string) for RSS 2.0 and
    `entry.authors` (list of dicts) for Atom. We coerce to a list[str].

Cross-source duplicates:
    Quantocracy aggregates Hudson & Thames / Robot Wealth / etc., and a
    paper can appear in both arXiv and a journal feed. The same item under
    multiple source_ids is kept on purpose — source identity is signal.
    Dedup is WITHIN a source.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any

import feedparser

from news_archive import http
from news_archive.collectors.base import utcnow
from news_archive.hashing import content_hash
from news_archive.literature import db as lit_db
from news_archive.literature.collectors.base import LitBaseCollector
from news_archive.literature.models import Paper


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


# ScienceDirect's RSS items omit <pubDate> entirely and embed the date in the
# description as "Publication date: Available online 15 April 2026" or
# "Publication date: July 2026" (issue-assigned). We parse both shapes as a
# last-resort fallback. Month-only dates land on day 1 — the brief's backtest
# contract uses GREATEST(source_published_at, source_fetched_at), so an early
# parsed date can never create look-ahead bias.
_MONTH_NAMES: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}
_PUBDATE_DAY_MONTH_YEAR_RE = re.compile(
    r"Publication date:[^<\n]*?(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})"
)
_PUBDATE_MONTH_YEAR_RE = re.compile(
    r"Publication date:[^<\n]*?([A-Za-z]+)\s+(\d{4})"
)

_AUTHORS_DESCRIPTION_RE = re.compile(
    r"Author\(s\):\s*([^<\n]+)",
    re.IGNORECASE,
)


def extract_authors_from_description(description: str | None) -> list[str]:
    """Parse a ScienceDirect-style `Author(s): A, B, C` clause. Returns []
    when not present. Names are whitespace-trimmed; trailing commas/periods
    from the description HTML are stripped."""
    if not description:
        return []
    m = _AUTHORS_DESCRIPTION_RE.search(description)
    if m is None:
        return []
    raw = m.group(1).strip()
    # Strip trailing HTML artefacts and punctuation
    raw = re.sub(r"</?[^>]+>", "", raw).strip().rstrip(".,;")
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def extract_pubdate_from_description(description: str | None) -> datetime | None:
    """Parse a ScienceDirect-style `Publication date:` clause out of an HTML
    description. Returns a UTC-midnight datetime, or None if no date matches.
    Day-month-year is preferred; falls back to month-year (first of month).
    """
    if not description:
        return None
    m = _PUBDATE_DAY_MONTH_YEAR_RE.search(description)
    if m:
        day_s, mon_s, year_s = m.group(1), m.group(2).lower(), m.group(3)
        mon = _MONTH_NAMES.get(mon_s)
        if mon is not None:
            try:
                return datetime(int(year_s), mon, int(day_s), tzinfo=UTC)
            except ValueError:
                pass
    m = _PUBDATE_MONTH_YEAR_RE.search(description)
    if m:
        mon = _MONTH_NAMES.get(m.group(1).lower())
        if mon is not None:
            try:
                return datetime(int(m.group(2)), mon, 1, tzinfo=UTC)
            except ValueError:
                pass
    return None


def extract_authors(entry: Any) -> list[str]:
    """Return author names as a list. Accepts both RSS 2.0 and Atom shapes."""
    # Atom: entry.authors = [{"name": "..."}, ...]
    authors = entry.get("authors") if isinstance(entry, dict) else None
    if authors is None and hasattr(entry, "get"):
        authors = entry.get("authors")
    out: list[str] = []
    if authors:
        for a in authors:
            if not isinstance(a, dict):
                continue
            name = (a.get("name") or "").strip()
            if name:
                out.append(name)
    if out:
        return out
    # RSS 2.0 fallback: single `author` string. WordPress emits email-ish or
    # "FirstName LastName"; store the raw value, downstream can clean it.
    author = entry.get("author")
    if isinstance(author, str) and author.strip():
        return [author.strip()]
    return []


def extract_categories(entry: Any) -> list[str]:
    """Return unique category terms from <category>/<tag> entries."""
    tags = entry.get("tags") or []
    out: list[str] = []
    seen: set[str] = set()
    for t in tags:
        if not isinstance(t, dict):
            continue
        term = (t.get("term") or "").strip()
        if term and term not in seen:
            seen.add(term)
            out.append(term)
    return out


def _entry_to_dict(entry: Any, *, feed_url: str) -> dict[str, Any]:
    """feedparser entries aren't JSON-serializable. Flatten what we care about."""
    out: dict[str, Any] = {"_source_feed": feed_url}
    for key in (
        "id", "guid", "link", "title", "summary", "description",
        "author", "published", "updated",
    ):
        val = entry.get(key)
        if val is not None:
            out[key] = val
    tags = entry.get("tags")
    if tags:
        out["tags"] = [t.get("term") for t in tags if isinstance(t, dict) and t.get("term")]
    authors = entry.get("authors")
    if authors:
        out["authors"] = [a.get("name") for a in authors if isinstance(a, dict) and a.get("name")]
    return out


def entry_to_paper(
    entry: Any,
    *,
    source_id: int,
    feed_url: str,
    fetched_at: datetime,
    logger: Any | None = None,
) -> Paper | None:
    """Map a parsed RSS entry to a Paper row. Returns None for unusable entries."""
    published = _parsed_time_to_utc(entry.get("published_parsed"))
    if published is None:
        # ScienceDirect (and any feed without <pubDate>) — try to recover the
        # date from the HTML description before giving up.
        published = extract_pubdate_from_description(
            entry.get("summary") or entry.get("description")
        )
    if published is None:
        if logger is not None:
            logger.warning(
                "item.skipped_no_pubdate",
                title=(entry.get("title") or "")[:120],
                link=entry.get("link"),
            )
        return None

    title = (entry.get("title") or "").strip()
    if not title:
        if logger is not None:
            logger.warning("item.skipped_no_title", link=entry.get("link"))
        return None

    url = entry.get("link") or None
    if not url:
        if logger is not None:
            logger.warning("item.skipped_no_url", title=title[:120])
        return None

    abstract = (entry.get("summary") or entry.get("description") or "").strip() or None
    external_id = entry.get("id") or entry.get("guid") or url

    authors = extract_authors(entry)
    if not authors:
        # ScienceDirect-style: Author(s) embedded in description HTML.
        authors = extract_authors_from_description(abstract)

    return Paper(
        source_id=source_id,
        external_id=external_id,
        url=url,
        pdf_url=None,
        title=title,
        authors=authors,
        abstract=abstract,
        categories=extract_categories(entry),
        keywords=[],
        source_published_at=published,
        source_fetched_at=fetched_at,
        raw_payload=_entry_to_dict(entry, feed_url=feed_url),
        content_hash=content_hash(title, abstract),
    )


class RssFeedCollector(LitBaseCollector):
    """Parameterized collector — slug + feed_url come from literature.sources.

    Instantiation:
        RssFeedCollector("blog_hudson_thames").run()
        RssFeedCollector("journal_jfe").run()

    The class does not declare `source_slug` as a class attribute; callers
    pass it to __init__ which sets the instance attribute before the base
    class initializer runs its `source_slug` lookup.
    """

    def __init__(self, source_slug: str) -> None:
        self.source_slug = source_slug  # set before super().__init__ uses it
        super().__init__()
        self.feed_url = lit_db.get_feed_url_by_slug(source_slug)

    def collect(self) -> Iterable[Paper]:
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
            paper = entry_to_paper(
                entry,
                source_id=self.source_id,
                feed_url=self.feed_url,
                fetched_at=fetched_at,
                logger=self.logger,
            )
            if paper is not None:
                yield paper

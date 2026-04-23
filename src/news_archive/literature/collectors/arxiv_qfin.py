"""arXiv quantitative-finance collector — Atom API at export.arxiv.org.

Source choice:
    arXiv's public API returns an Atom feed that `feedparser` parses cleanly.
    We filter by category to the five q-fin sub-areas relevant to systematic
    trading (trading, portfolio management, statistical/computational finance,
    risk management). Economics (q-fin.EC) and general finance (q-fin.GN) are
    excluded — they rarely produce strategy-level ideas.

Timestamp:
    * source_published_at = `<published>` (original submission).
      Revisions (v2, v3, ...) do not shift this. The version/updated_at is kept
      inside raw_payload so the archive can reconstruct revision history later.

Dedup:
    * external_id = versionless arXiv id (e.g. "2404.12345" — NOT "2404.12345v3").
      A revision therefore collapses into the existing row by external_id.
    * content_hash = SHA-256 of normalized (title + abstract). Title edits in
      later revisions are rare; abstract tweaks are more common. When both are
      stable the hash de-dups too.

Cadence: daily (arXiv publishes once a day per category set; hourly polling is
rude and pointless). See deploy/cron/news-pipeline.cron.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any, ClassVar

import feedparser

from news_archive import http
from news_archive.collectors.base import utcnow
from news_archive.hashing import content_hash
from news_archive.literature.collectors.base import LitBaseCollector
from news_archive.literature.models import Paper

ARXIV_API_URL = "https://export.arxiv.org/api/query"

DEFAULT_CATEGORIES: tuple[str, ...] = (
    "q-fin.TR",  # trading and market microstructure
    "q-fin.PM",  # portfolio management
    "q-fin.ST",  # statistical finance
    "q-fin.CP",  # computational finance
    "q-fin.RM",  # risk management
)

# arXiv ids come in two shapes. We strip trailing "vN" if present.
#   new:  2404.12345[vN]
#   old:  cond-mat/0612345[vN]
_VERSION_SUFFIX_RE = re.compile(r"v\d+$")
_ABS_URL_ID_RE = re.compile(r"/abs/(?P<id>[^?#\s]+?)$")


def strip_version(arxiv_id: str) -> str:
    """`'2404.12345v3'` → `'2404.12345'`. Leaves un-versioned ids unchanged."""
    return _VERSION_SUFFIX_RE.sub("", arxiv_id)


def extract_arxiv_id(entry: Any) -> str | None:
    """Parse the versionless arXiv id from an entry.

    Tries `<id>` first (always a canonical abs URL) then falls back to `<link>`.
    Returns None if neither matches the expected shape — upstream will skip.
    """
    for key in ("id", "link"):
        val = entry.get(key)
        if not val:
            continue
        m = _ABS_URL_ID_RE.search(val)
        if m:
            return strip_version(m.group("id"))
    return None


def extract_pdf_url(entry: Any) -> str | None:
    """Find the PDF link in the entry's `<link>` list. None if absent."""
    links = entry.get("links") or []
    for link in links:
        if not isinstance(link, dict):
            continue
        if link.get("type") == "application/pdf":
            return link.get("href")
        if (link.get("title") or "").lower() == "pdf":
            return link.get("href")
    return None


def extract_authors(entry: Any) -> list[str]:
    """feedparser exposes Atom `<author><name>` entries as a list of dicts."""
    authors = entry.get("authors") or []
    out: list[str] = []
    for a in authors:
        if not isinstance(a, dict):
            continue
        name = (a.get("name") or "").strip()
        if name:
            out.append(name)
    return out


def extract_categories(entry: Any) -> list[str]:
    """arXiv categories come through as `<category term="q-fin.TR">` tags."""
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
    """feedparser entries aren't JSON-serializable. Flatten the fields we want to keep."""
    out: dict[str, Any] = {}
    for key in (
        "id", "link", "title", "summary", "published", "updated",
        "arxiv_comment", "arxiv_primary_category", "arxiv_doi",
    ):
        val = entry.get(key)
        if val is not None:
            out[key] = val if not isinstance(val, dict) else dict(val)
    tags = entry.get("tags")
    if tags:
        out["tags"] = [t.get("term") for t in tags if isinstance(t, dict) and t.get("term")]
    authors = entry.get("authors")
    if authors:
        out["authors"] = [a.get("name") for a in authors if isinstance(a, dict) and a.get("name")]
    links = entry.get("links")
    if links:
        out["links"] = [
            {"rel": l.get("rel"), "type": l.get("type"), "title": l.get("title"), "href": l.get("href")}
            for l in links
            if isinstance(l, dict)
        ]
    return out


def build_search_query(categories: Iterable[str]) -> str:
    """`cat:q-fin.TR OR cat:q-fin.PM OR ...` — feeds the `search_query` param."""
    return " OR ".join(f"cat:{c}" for c in categories)


def parse_feed(raw: bytes) -> Any:
    """Thin wrapper so tests can target parsing directly."""
    return feedparser.parse(raw)


class ArxivQfinCollector(LitBaseCollector):
    source_slug = "arxiv_qfin"
    api_url: ClassVar[str] = ARXIV_API_URL
    categories: ClassVar[tuple[str, ...]] = DEFAULT_CATEGORIES
    max_results: ClassVar[int] = 100

    def _build_params(self) -> dict[str, Any]:
        return {
            "search_query": build_search_query(self.categories),
            "sortBy": "submittedDate",
            "sortOrder": "descending",
            "start": 0,
            "max_results": self.max_results,
        }

    def collect(self) -> Iterable[Paper]:
        raw = http.fetch_bytes(self.api_url, params=self._build_params())
        fetched_at = utcnow()

        feed = parse_feed(raw)
        if feed.bozo:
            self.logger.warning(
                "feed.parse_warning",
                bozo_reason=str(feed.bozo_exception),
                feed_url=self.api_url,
            )

        self.logger.info("feed.loaded", entries=len(feed.entries), feed_url=self.api_url)

        for entry in feed.entries:
            yield from self._entry_to_paper(entry, fetched_at=fetched_at)

    def _entry_to_paper(self, entry: Any, *, fetched_at: datetime) -> Iterable[Paper]:
        published = _parsed_time_to_utc(entry.get("published_parsed"))
        if published is None:
            self.logger.warning(
                "item.skipped_no_pubdate",
                title=(entry.get("title") or "")[:120],
                link=entry.get("link"),
            )
            return

        title = (entry.get("title") or "").strip()
        if not title:
            self.logger.warning("item.skipped_no_title", link=entry.get("link"))
            return

        arxiv_id = extract_arxiv_id(entry)
        if arxiv_id is None:
            self.logger.warning("item.skipped_no_id", title=title[:120])
            return

        url = entry.get("link") or None
        if not url:
            self.logger.warning("item.skipped_no_url", title=title[:120])
            return

        abstract = (entry.get("summary") or "").strip() or None
        pdf_url = extract_pdf_url(entry)
        authors = extract_authors(entry)
        categories = extract_categories(entry)

        yield Paper(
            source_id=self.source_id,
            external_id=arxiv_id,
            url=url,
            pdf_url=pdf_url,
            title=title,
            authors=authors,
            abstract=abstract,
            categories=categories,
            keywords=[],  # arXiv doesn't expose a keyword field
            source_published_at=published,
            source_fetched_at=fetched_at,
            raw_payload=_entry_to_dict(entry),
            content_hash=content_hash(title, abstract),
        )

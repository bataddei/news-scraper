"""SEC EDGAR Mag 7 filings collector — one atom feed per CIK.

EDGAR exposes a per-company atom feed at:

    https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<cik>&output=atom

Each entry is one filing (10-K, 10-Q, 8-K, 4, DEF 14A, etc.). The atom `<id>` is
of the form `urn:tag:sec.gov,2008:accession-number=0001140361-26-015711`; we use
the raw accession number as `external_id`, which is both shorter and the field
SEC's own URLs embed.

SEC fair-access rules:
    * Requires a descriptive User-Agent that includes contact info — handled
      upstream by `http._default_headers` via `settings.user_agent`.
    * Max 10 req/sec. We poll 7 CIKs per run with a ~0.5s sleep between feeds,
      which leaves plenty of headroom and avoids ever tripping rate-limit.

Per-CIK try/except (same pattern as BLS) so one company's 5xx doesn't kill the
other six feeds' inserts.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from datetime import UTC, datetime
from time import struct_time
from typing import Any

import feedparser

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

# ticker → (cik_10_digit, legal_name). CIKs are stable — the SEC never reassigns
# them, and the Mag 7 set is explicitly called out in the project brief.
MAG7: dict[str, tuple[str, str]] = {
    "AAPL": ("0000320193", "Apple Inc."),
    "MSFT": ("0000789019", "Microsoft Corporation"),
    "GOOGL": ("0001652044", "Alphabet Inc."),
    "AMZN": ("0001018724", "Amazon.com Inc."),
    "META": ("0001326801", "Meta Platforms Inc."),
    "NVDA": ("0001045810", "NVIDIA Corporation"),
    "TSLA": ("0001318605", "Tesla Inc."),
}

FEED_URL_TEMPLATE = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcompany&CIK={cik}&type=&dateb=&owner=include&count=40&output=atom"
)

# Fair-access politeness delay between per-CIK feed fetches.
INTER_FEED_SLEEP_SECONDS = 0.5


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


def _entry_to_dict(entry: Any, *, ticker: str, cik: str) -> dict[str, Any]:
    """Copy JSON-serializable fields + ticker/cik for durable replay."""
    out: dict[str, Any] = {
        "_ticker": ticker,
        "_cik": cik,
    }
    for key in (
        "title", "link", "id", "summary", "updated", "published",
        "accession-number", "filing-type", "filing-date", "filing-href",
        "form-name", "file-number", "items-desc", "size",
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


def _extract_form_type(entry: Any) -> str | None:
    """Prefer the explicit filing-type field; fall back to tag term."""
    ft = entry.get("filing-type")
    if ft:
        return str(ft).strip()
    tags = entry.get("tags") or []
    for t in tags:
        if t.get("label") == "form type" and t.get("term"):
            return str(t["term"]).strip()
    return None


class SECEdgarMag7Collector(BaseCollector):
    source_slug = "sec_edgar_mag7"
    tickers = MAG7

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        for i, (ticker, (cik, company)) in enumerate(self.tickers.items()):
            if i > 0:
                time.sleep(INTER_FEED_SLEEP_SECONDS)
            try:
                yield from self._collect_one_cik(ticker, cik, company)
            except Exception as e:
                # One company's feed failing must not kill the other six.
                self.logger.warning(
                    "feed.failed",
                    ticker=ticker,
                    cik=cik,
                    error=str(e),
                    error_type=type(e).__name__,
                )

    def _collect_one_cik(
        self, ticker: str, cik: str, company: str,
    ) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        feed_url = FEED_URL_TEMPLATE.format(cik=cik)
        raw = http.fetch_bytes(feed_url, headers={"Accept": "application/atom+xml"})
        fetched_at = utcnow()

        feed = feedparser.parse(raw)
        if feed.bozo:
            self.logger.warning(
                "feed.parse_warning",
                ticker=ticker,
                cik=cik,
                bozo_reason=str(feed.bozo_exception),
            )

        self.logger.info(
            "feed.loaded",
            ticker=ticker,
            cik=cik,
            entries=len(feed.entries),
        )

        for entry in feed.entries:
            published = _parsed_time_to_utc(entry.get("updated_parsed"))
            if published is None:
                self.logger.warning(
                    "item.skipped_no_updated",
                    ticker=ticker,
                    cik=cik,
                    title=entry.get("title", "")[:120],
                )
                continue

            headline = (entry.get("title") or "").strip()
            if not headline:
                self.logger.warning(
                    "item.skipped_no_headline",
                    ticker=ticker,
                    cik=cik,
                    link=entry.get("link"),
                )
                continue

            accession = entry.get("accession-number")
            # Fall back to parsing the urn-style id if accession-number is absent.
            if not accession:
                raw_id = entry.get("id") or ""
                if "accession-number=" in raw_id:
                    accession = raw_id.split("accession-number=", 1)[1].strip()
            external_id = accession or entry.get("id") or None

            form_type = _extract_form_type(entry)
            url = entry.get("link") or None
            body = entry.get("summary") or None
            raw_payload = _entry_to_dict(entry, ticker=ticker, cik=cik)

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
                ArticleEntity(entity_type="ticker", entity_value=ticker),
                ArticleEntity(entity_type="org", entity_value=company),
                ArticleEntity(entity_type="event", entity_value="SECFiling"),
            ]
            if form_type:
                entities.append(
                    ArticleEntity(entity_type="release_type", entity_value=form_type)
                )
            yield article, entities

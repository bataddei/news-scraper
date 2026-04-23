"""Unit tests for the arXiv q-fin collector.

Same discipline as the news-side tests: fixture-driven, no network, no DB.
The timestamp correctness check is the non-negotiable: bad `<published>`
parsing would poison backtest ordering when papers are joined against news.
"""

from __future__ import annotations

from datetime import UTC, datetime
from time import struct_time
from unittest.mock import patch

import feedparser

from news_archive.literature.collectors.arxiv_qfin import (
    ARXIV_API_URL,
    DEFAULT_CATEGORIES,
    _entry_to_dict,
    _parsed_time_to_utc,
    build_search_query,
    extract_arxiv_id,
    extract_authors,
    extract_categories,
    extract_pdf_url,
    strip_version,
)

# Two-entry fixture: one current (2025) paper in q-fin.TR, one older revision
# (v3) in q-fin.PM. Shapes mirror real export.arxiv.org responses.
FIXTURE_ATOM = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>ArXiv Query</title>
  <link href="http://export.arxiv.org/api/query" rel="self" />
  <updated>2026-04-23T08:00:00Z</updated>
  <entry>
    <id>http://arxiv.org/abs/2504.12345v1</id>
    <updated>2025-04-03T12:01:00Z</updated>
    <published>2025-04-02T18:00:00Z</published>
    <title>A simple momentum strategy revisited</title>
    <summary>We revisit the classic cross-sectional momentum factor on a
post-2020 universe and find the premium persists but is regime-dependent.</summary>
    <author><name>Alice Smith</name></author>
    <author><name>Bob Jones</name></author>
    <link href="http://arxiv.org/abs/2504.12345v1" rel="alternate" type="text/html" />
    <link title="pdf" href="http://arxiv.org/pdf/2504.12345v1" rel="related" type="application/pdf" />
    <category term="q-fin.TR" scheme="http://arxiv.org/schemas/atom" />
    <category term="q-fin.PM" scheme="http://arxiv.org/schemas/atom" />
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2401.00001v3</id>
    <updated>2025-02-10T10:00:00Z</updated>
    <published>2024-01-02T09:00:00Z</published>
    <title>Options market-making under stochastic volatility</title>
    <summary>A risk-neutral pricing framework for an options market-maker
exposed to stochastic volatility shocks.</summary>
    <author><name>Chen Wu</name></author>
    <link href="http://arxiv.org/abs/2401.00001v3" rel="alternate" type="text/html" />
    <link title="pdf" href="http://arxiv.org/pdf/2401.00001v3" rel="related" type="application/pdf" />
    <category term="q-fin.CP" scheme="http://arxiv.org/schemas/atom" />
  </entry>
</feed>
""".encode("utf-8")


class TestStripVersion:
    def test_strips_v1(self) -> None:
        assert strip_version("2504.12345v1") == "2504.12345"

    def test_strips_high_version(self) -> None:
        assert strip_version("2401.00001v12") == "2401.00001"

    def test_leaves_unversioned_id(self) -> None:
        assert strip_version("2401.00001") == "2401.00001"

    def test_leaves_old_format_unversioned(self) -> None:
        assert strip_version("cond-mat/0612345") == "cond-mat/0612345"

    def test_strips_version_from_old_format(self) -> None:
        assert strip_version("cond-mat/0612345v2") == "cond-mat/0612345"


class TestExtractArxivId:
    def test_from_id_field(self) -> None:
        entry = {"id": "http://arxiv.org/abs/2504.12345v1", "link": None}
        assert extract_arxiv_id(entry) == "2504.12345"

    def test_falls_back_to_link(self) -> None:
        entry = {"id": None, "link": "http://arxiv.org/abs/2401.00001v3"}
        assert extract_arxiv_id(entry) == "2401.00001"

    def test_returns_none_on_unparseable(self) -> None:
        assert extract_arxiv_id({"id": "garbage", "link": None}) is None


class TestExtractPdfUrl:
    def test_finds_application_pdf_link(self) -> None:
        entry = {
            "links": [
                {"href": "http://arxiv.org/abs/2504.12345v1", "rel": "alternate", "type": "text/html"},
                {"href": "http://arxiv.org/pdf/2504.12345v1", "rel": "related",
                 "type": "application/pdf", "title": "pdf"},
            ]
        }
        assert extract_pdf_url(entry) == "http://arxiv.org/pdf/2504.12345v1"

    def test_returns_none_when_absent(self) -> None:
        entry = {"links": [{"href": "http://arxiv.org/abs/x", "rel": "alternate", "type": "text/html"}]}
        assert extract_pdf_url(entry) is None

    def test_tolerates_missing_links(self) -> None:
        assert extract_pdf_url({}) is None


class TestExtractAuthors:
    def test_returns_names_in_order(self) -> None:
        entry = {"authors": [{"name": "Alice Smith"}, {"name": "Bob Jones"}]}
        assert extract_authors(entry) == ["Alice Smith", "Bob Jones"]

    def test_skips_empty_names(self) -> None:
        entry = {"authors": [{"name": ""}, {"name": "Alice"}]}
        assert extract_authors(entry) == ["Alice"]

    def test_empty_when_missing(self) -> None:
        assert extract_authors({}) == []


class TestExtractCategories:
    def test_returns_unique_terms(self) -> None:
        entry = {"tags": [{"term": "q-fin.TR"}, {"term": "q-fin.PM"}, {"term": "q-fin.TR"}]}
        assert extract_categories(entry) == ["q-fin.TR", "q-fin.PM"]


class TestBuildSearchQuery:
    def test_joins_with_or(self) -> None:
        assert build_search_query(["q-fin.TR", "q-fin.PM"]) == "cat:q-fin.TR OR cat:q-fin.PM"

    def test_default_categories_covered(self) -> None:
        q = build_search_query(DEFAULT_CATEGORIES)
        for cat in ("q-fin.TR", "q-fin.PM", "q-fin.ST", "q-fin.CP", "q-fin.RM"):
            assert f"cat:{cat}" in q


class TestParsedTimeToUtc:
    def test_converts_struct_time(self) -> None:
        st = struct_time((2025, 4, 2, 18, 0, 0, 2, 92, 0))
        result = _parsed_time_to_utc(st)
        assert result == datetime(2025, 4, 2, 18, 0, 0, tzinfo=UTC)
        assert result.tzinfo is not None

    def test_none_returns_none(self) -> None:
        assert _parsed_time_to_utc(None) is None


class TestFeedParsing:
    def test_published_parses_to_utc_datetime(self) -> None:
        feed = feedparser.parse(FIXTURE_ATOM)
        first = feed.entries[0]
        published = _parsed_time_to_utc(first.get("published_parsed"))
        assert published == datetime(2025, 4, 2, 18, 0, 0, tzinfo=UTC)

    def test_two_entries_parsed(self) -> None:
        feed = feedparser.parse(FIXTURE_ATOM)
        assert len(feed.entries) == 2


class TestEntryToDict:
    def test_serializes_core_fields_jsonable(self) -> None:
        import json
        feed = feedparser.parse(FIXTURE_ATOM)
        d = _entry_to_dict(feed.entries[0])
        assert d["title"].startswith("A simple momentum")
        assert "q-fin.TR" in d["tags"]
        assert "Alice Smith" in d["authors"]
        # Must round-trip through JSON because raw_payload is stored as jsonb.
        json.dumps(d)


class TestCollector:
    """Exercise collect() end-to-end against fixture bytes, with DB stubbed out."""

    def test_collect_yields_valid_papers(self) -> None:
        with patch(
            "news_archive.literature.collectors.base.lit_db.get_source_id_by_slug",
            return_value=77,
        ):
            with patch(
                "news_archive.literature.collectors.arxiv_qfin.http.fetch_bytes",
                return_value=FIXTURE_ATOM,
            ):
                from news_archive.literature.collectors.arxiv_qfin import ArxivQfinCollector
                collector = ArxivQfinCollector()
                papers = list(collector.collect())

        assert len(papers) == 2

        p1 = papers[0]
        assert p1.source_id == 77
        assert p1.external_id == "2504.12345"  # version stripped
        assert p1.title.startswith("A simple momentum")
        assert p1.authors == ["Alice Smith", "Bob Jones"]
        assert "q-fin.TR" in p1.categories and "q-fin.PM" in p1.categories
        assert p1.source_published_at == datetime(2025, 4, 2, 18, 0, 0, tzinfo=UTC)
        assert p1.source_fetched_at.tzinfo is not None
        assert p1.pdf_url == "http://arxiv.org/pdf/2504.12345v1"
        assert p1.url == "http://arxiv.org/abs/2504.12345v1"
        assert len(p1.content_hash) == 64

        p2 = papers[1]
        # Revision (v3) — external_id must still collapse to versionless id.
        assert p2.external_id == "2401.00001"
        # source_published_at is the ORIGINAL submission, not the latest update.
        assert p2.source_published_at == datetime(2024, 1, 2, 9, 0, 0, tzinfo=UTC)

    def test_collect_skips_entry_missing_published(self) -> None:
        broken = FIXTURE_ATOM.replace(
            b"<published>2025-04-02T18:00:00Z</published>",
            b"",
            1,
        )
        assert broken != FIXTURE_ATOM
        with patch(
            "news_archive.literature.collectors.base.lit_db.get_source_id_by_slug",
            return_value=77,
        ):
            with patch(
                "news_archive.literature.collectors.arxiv_qfin.http.fetch_bytes",
                return_value=broken,
            ):
                from news_archive.literature.collectors.arxiv_qfin import ArxivQfinCollector
                collector = ArxivQfinCollector()
                papers = list(collector.collect())

        # Second entry still parses; the broken one is skipped, not fabricated.
        assert len(papers) == 1
        assert papers[0].external_id == "2401.00001"


def test_api_url_is_the_public_arxiv_endpoint() -> None:
    assert ARXIV_API_URL == "https://export.arxiv.org/api/query"

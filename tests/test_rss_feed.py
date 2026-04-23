"""Unit tests for the generic RSS/Atom feed collector.

Fixture is a WordPress-shaped RSS 2.0 feed — representative of the blog
sources. The same collector class also drives journal RSS (ScienceDirect)
where the shape is close enough that no separate code path is needed.
Exercises the parser directly; DB and HTTP are mocked.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import feedparser

from news_archive.literature.collectors.rss_feed import (
    _entry_to_dict,
    _parsed_time_to_utc,
    entry_to_paper,
    extract_authors,
    extract_categories,
)

FIXTURE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Hudson &amp; Thames — Quant Research Blog</title>
    <link>https://hudsonthames.org/</link>
    <description>Research notes</description>
    <item>
      <title>Bayesian optimization of trading strategy hyperparameters</title>
      <link>https://hudsonthames.org/bayesian-optimization-trading/</link>
      <guid isPermaLink="false">https://hudsonthames.org/?p=12345</guid>
      <pubDate>Mon, 14 Apr 2026 09:00:00 +0000</pubDate>
      <dc:creator>Jane Researcher</dc:creator>
      <description>A walkthrough of using Gaussian processes to tune a mean-reversion strategy.</description>
      <category>optimization</category>
      <category>bayesian</category>
    </item>
    <item>
      <title>Regime detection for pairs trading</title>
      <link>https://hudsonthames.org/regime-pairs/</link>
      <guid isPermaLink="false">https://hudsonthames.org/?p=12400</guid>
      <pubDate>Tue, 15 Apr 2026 12:30:00 +0000</pubDate>
      <dc:creator>Alex Quant</dc:creator>
      <description>Hidden Markov models on top of a cointegration spread.</description>
      <category>regime</category>
    </item>
  </channel>
</rss>
""".encode("utf-8")


class TestParsedTimeToUtc:
    def test_returns_none_for_none(self) -> None:
        assert _parsed_time_to_utc(None) is None


class TestExtractAuthors:
    def test_prefers_atom_authors_list(self) -> None:
        entry = {"authors": [{"name": "A"}, {"name": "B"}]}
        assert extract_authors(entry) == ["A", "B"]

    def test_falls_back_to_rss_author_string(self) -> None:
        # feedparser also mirrors <dc:creator> to `author` for RSS feeds.
        entry = {"author": "Jane Researcher"}
        assert extract_authors(entry) == ["Jane Researcher"]

    def test_returns_empty_when_missing(self) -> None:
        assert extract_authors({}) == []


class TestExtractCategories:
    def test_unique_terms_in_order(self) -> None:
        entry = {"tags": [{"term": "optimization"}, {"term": "bayesian"}, {"term": "optimization"}]}
        assert extract_categories(entry) == ["optimization", "bayesian"]

    def test_empty_when_missing(self) -> None:
        assert extract_categories({}) == []


class TestFeedParsing:
    def test_two_items_with_tz_aware_pubdates(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        assert len(feed.entries) == 2
        p0 = _parsed_time_to_utc(feed.entries[0].get("published_parsed"))
        p1 = _parsed_time_to_utc(feed.entries[1].get("published_parsed"))
        assert p0 == datetime(2026, 4, 14, 9, 0, 0, tzinfo=UTC)
        assert p1 == datetime(2026, 4, 15, 12, 30, 0, tzinfo=UTC)


class TestEntryToDict:
    def test_serializes_and_round_trips_through_json(self) -> None:
        import json
        feed = feedparser.parse(FIXTURE_RSS)
        d = _entry_to_dict(feed.entries[0], feed_url="https://hudsonthames.org/feed/")
        assert d["_source_feed"] == "https://hudsonthames.org/feed/"
        assert d["title"].startswith("Bayesian")
        json.dumps(d)


class TestEntryToPaper:
    def test_happy_path_builds_paper(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        fetched = datetime(2026, 4, 23, 9, 0, 0, tzinfo=UTC)
        paper = entry_to_paper(
            feed.entries[0],
            source_id=42,
            feed_url="https://hudsonthames.org/feed/",
            fetched_at=fetched,
        )
        assert paper is not None
        assert paper.source_id == 42
        assert paper.title.startswith("Bayesian")
        assert paper.url == "https://hudsonthames.org/bayesian-optimization-trading/"
        assert paper.external_id == "https://hudsonthames.org/?p=12345"
        assert paper.authors == ["Jane Researcher"]
        assert set(paper.categories) == {"optimization", "bayesian"}
        assert paper.source_published_at == datetime(2026, 4, 14, 9, 0, 0, tzinfo=UTC)
        assert paper.source_fetched_at == fetched
        assert len(paper.content_hash) == 64
        assert paper.pdf_url is None  # blogs don't expose PDFs

    def test_skips_entry_missing_pubdate(self) -> None:
        # Strip the first pubDate
        broken = FIXTURE_RSS.replace(b"<pubDate>Mon, 14 Apr 2026 09:00:00 +0000</pubDate>", b"", 1)
        feed = feedparser.parse(broken)
        paper = entry_to_paper(
            feed.entries[0],
            source_id=42,
            feed_url="https://example.com/feed/",
            fetched_at=datetime(2026, 4, 23, 9, 0, 0, tzinfo=UTC),
        )
        assert paper is None

    def test_skips_entry_missing_url(self) -> None:
        broken = FIXTURE_RSS.replace(
            b"<link>https://hudsonthames.org/bayesian-optimization-trading/</link>",
            b"<link></link>",
            1,
        )
        feed = feedparser.parse(broken)
        paper = entry_to_paper(
            feed.entries[0],
            source_id=42,
            feed_url="https://example.com/feed/",
            fetched_at=datetime(2026, 4, 23, 9, 0, 0, tzinfo=UTC),
        )
        assert paper is None


class TestRssFeedCollector:
    """Exercise collect() end-to-end against fixture bytes, with DB+HTTP stubbed."""

    def test_collect_yields_two_papers(self) -> None:
        with patch(
            "news_archive.literature.collectors.base.lit_db.get_source_id_by_slug",
            return_value=55,
        ), patch(
            "news_archive.literature.collectors.rss_feed.lit_db.get_feed_url_by_slug",
            return_value="https://hudsonthames.org/feed/",
        ), patch(
            "news_archive.literature.collectors.rss_feed.http.fetch_bytes",
            return_value=FIXTURE_RSS,
        ):
            from news_archive.literature.collectors.rss_feed import RssFeedCollector
            collector = RssFeedCollector("blog_hudson_thames")
            papers = list(collector.collect())

        assert len(papers) == 2
        assert all(p.source_id == 55 for p in papers)
        assert all(p.source_fetched_at.tzinfo is not None for p in papers)
        assert papers[0].external_id != papers[1].external_id

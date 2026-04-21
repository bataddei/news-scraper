"""Unit tests for the press-wire collectors."""

from __future__ import annotations

import json
from unittest.mock import patch

import feedparser

from news_archive.collectors.wires import (
    GlobeNewswireCollector,
    PRNewswireCollector,
    RssWireCollector,
    _entry_to_dict,
    extract_tickers_from_tags,
)

GNW_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>GlobeNewswire - News about Public Companies</title>
    <link>https://www.globenewswire.com/</link>
    <description>Public company press releases.</description>
    <item>
      <title>S&amp;P upgrades SiriusPoint's Insurance Subsidiaries to 'A'</title>
      <link>https://www.globenewswire.com/news-release/2026/04/21/3278316/0/en/example.html</link>
      <guid isPermaLink="true">https://www.globenewswire.com/news-release/2026/04/21/3278316/0/en/example.html</guid>
      <pubDate>Tue, 21 Apr 2026 17:22:00 GMT</pubDate>
      <description>HAMILTON, Bermuda ... announced today ...</description>
      <category>NYSE:SPNT</category>
      <category>BMG8192H1060</category>
      <category>Product / Services Announcement</category>
    </item>
    <item>
      <title>Royalty Pharma Launches Global Translational Prize</title>
      <link>https://www.globenewswire.com/news-release/2026/04/21/3278310/0/en/royalty.html</link>
      <guid isPermaLink="true">https://www.globenewswire.com/news-release/2026/04/21/3278310/0/en/royalty.html</guid>
      <pubDate>Tue, 21 Apr 2026 17:00:00 GMT</pubDate>
      <description>NEW YORK, April 21, 2026 ...</description>
      <category>Nasdaq:RPRX</category>
      <category>GB00BMVP7Y09</category>
      <category>Contests/Awards</category>
    </item>
    <item>
      <title>Private company announcement (no ticker tag)</title>
      <link>https://www.globenewswire.com/news-release/2026/04/21/3278000/0/en/private.html</link>
      <guid>https://www.globenewswire.com/news-release/2026/04/21/3278000/0/en/private.html</guid>
      <pubDate>Tue, 21 Apr 2026 16:30:00 GMT</pubDate>
      <description>...</description>
      <category>Contests/Awards</category>
    </item>
  </channel>
</rss>
""".encode("utf-8")

PRN_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>All News Releases</title>
    <link>https://www.prnewswire.com/</link>
    <description>PR Newswire all releases.</description>
    <item>
      <title>Example Consulting Firm Names New Partner</title>
      <link>https://www.prnewswire.com/news-releases/example-302749046.html</link>
      <guid>https://www.prnewswire.com/news-releases/example-302749046.html</guid>
      <pubDate>Tue, 21 Apr 2026 17:30:00 +0000</pubDate>
      <description>CHICAGO, April 21, 2026 ...</description>
      <category>AWD</category>
      <category>PER</category>
    </item>
  </channel>
</rss>
""".encode("utf-8")


class TestExtractTickersFromTags:
    def test_nyse_prefix(self) -> None:
        tags = [{"term": "NYSE:SPNT", "label": None}]
        assert extract_tickers_from_tags(tags) == ["SPNT"]

    def test_case_insensitive_exchange(self) -> None:
        tags = [{"term": "Nasdaq:RPRX", "label": None}]
        assert extract_tickers_from_tags(tags) == ["RPRX"]

    def test_skips_isin_and_categories(self) -> None:
        tags = [
            {"term": "NYSE:SPNT", "label": None},
            {"term": "BMG8192H1060", "label": None},
            {"term": "Product / Services Announcement", "label": None},
        ]
        assert extract_tickers_from_tags(tags) == ["SPNT"]

    def test_deduplicates(self) -> None:
        tags = [
            {"term": "NYSE:AAPL", "label": None},
            {"term": "Nasdaq:AAPL", "label": None},
        ]
        assert extract_tickers_from_tags(tags) == ["AAPL"]

    def test_none_and_empty_safe(self) -> None:
        assert extract_tickers_from_tags(None) == []
        assert extract_tickers_from_tags([]) == []

    def test_rejects_unknown_exchange(self) -> None:
        # We don't want to accept arbitrary "Foo:Bar" tags as tickers.
        tags = [{"term": "Product:Announcement", "label": None}]
        assert extract_tickers_from_tags(tags) == []

    def test_preserves_order(self) -> None:
        tags = [
            {"term": "NYSE:BBBB", "label": None},
            {"term": "Nasdaq:AAAA", "label": None},
        ]
        assert extract_tickers_from_tags(tags) == ["BBBB", "AAAA"]


class TestEntryToDict:
    def test_json_serializable(self) -> None:
        feed = feedparser.parse(GNW_FIXTURE)
        for entry in feed.entries:
            d = _entry_to_dict(entry, feed_url="https://example.com/feed", wire="GlobeNewswire")
            assert d["_wire"] == "GlobeNewswire"
            json.dumps(d)  # must not raise


class TestGlobeNewswireCollector:
    def test_three_articles_and_ticker_tags(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=9):
            with patch(
                "news_archive.collectors.wires.http.fetch_bytes",
                return_value=GNW_FIXTURE,
            ):
                results = list(GlobeNewswireCollector().collect())

        assert len(results) == 3

        by_headline = {a.headline: ents for a, ents in results}
        # Articles with NYSE:SPNT / Nasdaq:RPRX tags must get ticker entities.
        spnt_ents = by_headline["S&P upgrades SiriusPoint's Insurance Subsidiaries to 'A'"]
        assert any(e.entity_type == "ticker" and e.entity_value == "SPNT" for e in spnt_ents)

        rprx_ents = by_headline["Royalty Pharma Launches Global Translational Prize"]
        assert any(e.entity_type == "ticker" and e.entity_value == "RPRX" for e in rprx_ents)

        # Article without a ticker tag must still yield event+org but no ticker.
        private_ents = by_headline["Private company announcement (no ticker tag)"]
        assert not any(e.entity_type == "ticker" for e in private_ents)

        # Every article carries the common event=PressRelease and org=GlobeNewswire.
        for _a, ents in results:
            assert any(e.entity_type == "event" and e.entity_value == "PressRelease" for e in ents)
            assert any(e.entity_type == "org" and e.entity_value == "GlobeNewswire" for e in ents)

    def test_timestamps_and_content_hash(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=9):
            with patch(
                "news_archive.collectors.wires.http.fetch_bytes",
                return_value=GNW_FIXTURE,
            ):
                results = list(GlobeNewswireCollector().collect())
        for article, _ in results:
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            assert len(article.content_hash) == 64


class TestPRNewswireCollector:
    def test_yields_article_without_ticker_entity(self) -> None:
        """PRN tags are industry codes, not ticker symbols — must not emit ticker entities."""
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=10):
            with patch(
                "news_archive.collectors.wires.http.fetch_bytes",
                return_value=PRN_FIXTURE,
            ):
                results = list(PRNewswireCollector().collect())

        assert len(results) == 1
        article, ents = results[0]
        assert article.headline == "Example Consulting Firm Names New Partner"
        assert any(e.entity_type == "event" and e.entity_value == "PressRelease" for e in ents)
        assert any(e.entity_type == "org" and e.entity_value == "PR Newswire" for e in ents)
        # AWD/PER are industry codes — must NOT be misparsed as tickers.
        assert not any(e.entity_type == "ticker" for e in ents)


def test_subclasses_have_required_config() -> None:
    for cls in (PRNewswireCollector, GlobeNewswireCollector):
        assert cls.source_slug.startswith("wire_")
        assert cls.feed_url.startswith("https://")
        assert cls.wire_name


def test_base_class_has_empty_config() -> None:
    # Abstract-ish guard: RssWireCollector itself mustn't accidentally run.
    assert RssWireCollector.feed_url == ""
    assert RssWireCollector.wire_name == ""

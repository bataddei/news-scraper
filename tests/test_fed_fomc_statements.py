"""Unit tests for FOMC RSS parsing.

These tests exercise the parser directly against fixture XML — no network,
no DB. The rule from the brief is: "given this RSS snippet, do we extract
the right timestamp?" — and we care most about the timestamp because
lookahead bias from bad timestamps would poison every future backtest.
"""

from __future__ import annotations

from datetime import UTC, datetime
from time import struct_time
from unittest.mock import patch

import feedparser

from news_archive.collectors.fed_fomc_statements import (
    FEED_URL,
    _entry_to_dict,
    _parsed_time_to_utc,
)

FIXTURE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Federal Reserve — Monetary Policy</title>
    <link>https://www.federalreserve.gov/</link>
    <description>Monetary policy press releases.</description>
    <item>
      <title>Federal Reserve issues FOMC statement</title>
      <link>https://www.federalreserve.gov/newsevents/pressreleases/monetary20240612a.htm</link>
      <guid isPermaLink="true">https://www.federalreserve.gov/newsevents/pressreleases/monetary20240612a.htm</guid>
      <pubDate>Wed, 12 Jun 2024 18:00:00 GMT</pubDate>
      <description>Recent indicators suggest that economic activity has continued to expand at a solid pace.</description>
    </item>
    <item>
      <title>Minutes of the Federal Open Market Committee, June 11-12, 2024</title>
      <link>https://www.federalreserve.gov/monetarypolicy/fomcminutes20240612.htm</link>
      <guid isPermaLink="true">https://www.federalreserve.gov/monetarypolicy/fomcminutes20240612.htm</guid>
      <pubDate>Wed, 03 Jul 2024 18:00:00 GMT</pubDate>
      <description>A summary of economic projections was also released.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")


class TestParsedTimeToUtc:
    def test_converts_struct_time(self) -> None:
        st = struct_time((2024, 6, 12, 18, 0, 0, 2, 164, 0))
        result = _parsed_time_to_utc(st)
        assert result == datetime(2024, 6, 12, 18, 0, 0, tzinfo=UTC)
        assert result.tzinfo is not None

    def test_none_returns_none(self) -> None:
        assert _parsed_time_to_utc(None) is None


class TestFeedParsing:
    def test_pubdate_parses_to_utc_datetime(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        first = feed.entries[0]
        published = _parsed_time_to_utc(first.get("published_parsed"))
        assert published == datetime(2024, 6, 12, 18, 0, 0, tzinfo=UTC)

    def test_two_entries_parsed(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        assert len(feed.entries) == 2

    def test_minutes_entry_distinct_from_statement(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        statement, minutes = feed.entries
        assert "FOMC statement" in statement.title
        assert "Minutes" in minutes.title
        # Minutes are published weeks after the statement — make sure that drift is preserved.
        st_pub = _parsed_time_to_utc(statement.published_parsed)
        mn_pub = _parsed_time_to_utc(minutes.published_parsed)
        assert st_pub is not None and mn_pub is not None
        assert (mn_pub - st_pub).days == 21


class TestEntryToDict:
    def test_serializes_core_fields(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        entry = feed.entries[0]
        d = _entry_to_dict(entry)
        assert d["title"] == "Federal Reserve issues FOMC statement"
        assert d["link"].endswith("monetary20240612a.htm")
        assert "Recent indicators" in d["description"] or "Recent indicators" in d.get("summary", "")
        # must be JSON-serializable (raw_payload is stored as jsonb)
        import json
        json.dumps(d)


class TestCollector:
    """Exercise collect() end-to-end against fixture bytes, with DB stubbed out."""

    def test_collect_yields_valid_articles(self) -> None:
        # Stub DB lookup so BaseCollector.__init__ doesn't hit Postgres.
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=99):
            with patch(
                "news_archive.collectors.fed_fomc_statements.http.fetch_bytes",
                return_value=FIXTURE_RSS,
            ):
                # Import here so the patch on db.get_source_id_by_slug takes effect at __init__.
                from news_archive.collectors.fed_fomc_statements import (
                    FOMCStatementsCollector,
                )
                collector = FOMCStatementsCollector()
                results = list(collector.collect())

        assert len(results) == 2
        for article, entities in results:
            assert article.source_id == 99
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            assert len(article.content_hash) == 64
            assert article.language == "en"
            types = {e.entity_type for e in entities}
            assert types == {"event", "org"}

    def test_collect_skips_entry_missing_pubdate(self) -> None:
        broken = FIXTURE_RSS.replace(
            b"<pubDate>Wed, 12 Jun 2024 18:00:00 GMT</pubDate>",
            b"",
            1,
        )
        assert broken != FIXTURE_RSS
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=99):
            with patch(
                "news_archive.collectors.fed_fomc_statements.http.fetch_bytes",
                return_value=broken,
            ):
                from news_archive.collectors.fed_fomc_statements import (
                    FOMCStatementsCollector,
                )
                collector = FOMCStatementsCollector()
                results = list(collector.collect())

        # Second entry still parses; the broken one is skipped, not fabricated.
        assert len(results) == 1


def test_feed_url_is_the_fed_monetary_feed() -> None:
    assert FEED_URL == "https://www.federalreserve.gov/feeds/press_monetary.xml"

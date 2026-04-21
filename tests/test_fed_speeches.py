"""Unit tests for Fed speeches collector — speaker extraction and parsing."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import feedparser

from news_archive.collectors.fed_speeches import (
    FEED_URL,
    _entry_to_dict,
    _is_testimony,
    _parsed_time_to_utc,
    extract_speaker_last_name,
)

FIXTURE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Federal Reserve — Speeches and Testimony</title>
    <link>https://www.federalreserve.gov/</link>
    <description>All speakers.</description>
    <item>
      <title>Waller, One Transitory Shock After Another</title>
      <link>https://www.federalreserve.gov/newsevents/speech/waller20260417a.htm</link>
      <guid isPermaLink="true">https://www.federalreserve.gov/newsevents/speech/waller20260417a.htm</guid>
      <pubDate>Fri, 17 Apr 2026 18:00:00 GMT</pubDate>
      <description>Speech at the David Kaserman Memorial Lecture.</description>
    </item>
    <item>
      <title>Powell, Monetary Policy and the Economy</title>
      <link>https://www.federalreserve.gov/newsevents/testimony/powell20260312a.htm</link>
      <guid isPermaLink="true">https://www.federalreserve.gov/newsevents/testimony/powell20260312a.htm</guid>
      <pubDate>Thu, 12 Mar 2026 14:00:00 GMT</pubDate>
      <description>Testimony before the Senate Committee on Banking.</description>
    </item>
    <item>
      <title>Jefferson, Economic Outlook and the Labor Market</title>
      <link>https://www.federalreserve.gov/newsevents/speech/jefferson20260407a.htm</link>
      <guid isPermaLink="true">https://www.federalreserve.gov/newsevents/speech/jefferson20260407a.htm</guid>
      <pubDate>Tue, 7 Apr 2026 21:50:00 GMT</pubDate>
      <description>Speech at the College of Business Administration.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")


class TestExtractSpeakerLastName:
    def test_speech_url(self) -> None:
        url = "https://www.federalreserve.gov/newsevents/speech/waller20260417a.htm"
        assert extract_speaker_last_name(url) == "Waller"

    def test_testimony_url(self) -> None:
        url = "https://www.federalreserve.gov/newsevents/testimony/powell20260312a.htm"
        assert extract_speaker_last_name(url) == "Powell"

    def test_multi_letter_name(self) -> None:
        url = "https://www.federalreserve.gov/newsevents/speech/jefferson20260407a.htm"
        assert extract_speaker_last_name(url) == "Jefferson"

    def test_none_url_returns_none(self) -> None:
        assert extract_speaker_last_name(None) is None

    def test_unrelated_url_returns_none(self) -> None:
        assert extract_speaker_last_name("https://example.com/some/path") is None

    def test_empty_string_returns_none(self) -> None:
        assert extract_speaker_last_name("") is None


class TestIsTestimony:
    def test_speech_url(self) -> None:
        assert _is_testimony("https://www.federalreserve.gov/newsevents/speech/waller20260417a.htm") is False

    def test_testimony_url(self) -> None:
        assert _is_testimony("https://www.federalreserve.gov/newsevents/testimony/powell20260312a.htm") is True

    def test_none(self) -> None:
        assert _is_testimony(None) is False


class TestFeedParsing:
    def test_three_entries(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        assert len(feed.entries) == 3

    def test_pubdates_are_utc(self) -> None:
        feed = feedparser.parse(FIXTURE_RSS)
        for entry in feed.entries:
            dt = _parsed_time_to_utc(entry.get("published_parsed"))
            assert dt is not None
            assert dt.tzinfo is not None
            assert dt.utcoffset() == datetime.now(UTC).utcoffset()

    def test_entry_to_dict_json_safe(self) -> None:
        import json
        feed = feedparser.parse(FIXTURE_RSS)
        for entry in feed.entries:
            d = _entry_to_dict(entry)
            json.dumps(d)  # must not raise


class TestCollector:
    def test_collect_yields_articles_with_speakers(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=42):
            with patch(
                "news_archive.collectors.fed_speeches.http.fetch_bytes",
                return_value=FIXTURE_RSS,
            ):
                from news_archive.collectors.fed_speeches import FedSpeechesCollector
                results = list(FedSpeechesCollector().collect())

        assert len(results) == 3

        by_speaker = {
            next(e.entity_value for e in ents if e.entity_type == "person"): (art, ents)
            for art, ents in results
        }
        assert set(by_speaker.keys()) == {"Waller", "Powell", "Jefferson"}

        # The Powell entry is testimony — should be tagged FedTestimony, not FedSpeech.
        _, powell_entities = by_speaker["Powell"]
        kinds = {e.entity_value for e in powell_entities if e.entity_type == "event"}
        assert kinds == {"FedTestimony"}

        # Waller is a speech.
        _, waller_entities = by_speaker["Waller"]
        waller_kinds = {e.entity_value for e in waller_entities if e.entity_type == "event"}
        assert waller_kinds == {"FedSpeech"}

    def test_every_article_has_required_timestamps(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=42):
            with patch(
                "news_archive.collectors.fed_speeches.http.fetch_bytes",
                return_value=FIXTURE_RSS,
            ):
                from news_archive.collectors.fed_speeches import FedSpeechesCollector
                results = list(FedSpeechesCollector().collect())

        for article, _ in results:
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            assert len(article.content_hash) == 64


def test_feed_url() -> None:
    assert FEED_URL == "https://www.federalreserve.gov/feeds/speeches_and_testimony.xml"

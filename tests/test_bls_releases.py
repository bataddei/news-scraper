"""Unit tests for the BLS releases collector."""

from __future__ import annotations

import json
from unittest.mock import patch

import feedparser

from news_archive.collectors.bls_releases import (
    FEEDS,
    _entry_to_dict,
    _parsed_time_to_utc,
)

CPI_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Consumer Price Index</title>
    <link>https://www.bls.gov/cpi/</link>
    <description>Monthly CPI releases.</description>
    <item>
      <title>CPI for all items rises 0.9% in March; gasoline up</title>
      <link>https://www.bls.gov/news.release/archives/cpi_04102026.htm</link>
      <guid>cpi-2026_04_10__08_30_00</guid>
      <pubDate>2026-04-10T08:30:00Z</pubDate>
      <description>In March, the Consumer Price Index for All Urban Consumers rose 0.9 percent.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")

EMPSIT_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Employment Situation</title>
    <link>https://www.bls.gov/empsit/</link>
    <description>Monthly jobs report.</description>
    <item>
      <title>Payroll employment increases by 178,000 in March</title>
      <link>https://www.bls.gov/news.release/archives/empsit_04042026.htm</link>
      <guid>empsit-2026_04_04__08_30_00</guid>
      <pubDate>2026-04-04T08:30:00Z</pubDate>
      <description>Total nonfarm payroll employment rose by 178,000 in March.</description>
    </item>
  </channel>
</rss>
""".encode("utf-8")


class TestFeedsMap:
    def test_all_five_feeds_listed(self) -> None:
        assert set(FEEDS.values()) == {
            "CPI", "PPI", "EmploymentSituation", "JOLTS", "RealEarnings",
        }

    def test_all_urls_are_bls(self) -> None:
        for url in FEEDS:
            assert url.startswith("https://www.bls.gov/feed/")
            assert url.endswith(".rss")


class TestEntryToDict:
    def test_embeds_feed_and_release_type(self) -> None:
        feed = feedparser.parse(CPI_FIXTURE)
        entry = feed.entries[0]
        d = _entry_to_dict(entry, feed_url="https://www.bls.gov/feed/cpi.rss", release_type="CPI")
        assert d["_source_feed"] == "https://www.bls.gov/feed/cpi.rss"
        assert d["_release_type"] == "CPI"
        assert d["title"].startswith("CPI for all items")
        json.dumps(d)  # must be JSON-serializable


class TestCollector:
    def test_multi_feed_aggregation(self) -> None:
        """Two feeds → two articles, each tagged with the right release_type."""
        def fake_fetch(url: str) -> bytes:
            if "cpi.rss" in url:
                return CPI_FIXTURE
            if "empsit.rss" in url:
                return EMPSIT_FIXTURE
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=7):
            with patch(
                "news_archive.collectors.bls_releases.http.fetch_bytes",
                side_effect=fake_fetch,
            ):
                with patch.dict(
                    "news_archive.collectors.bls_releases.FEEDS",
                    {
                        "https://www.bls.gov/feed/cpi.rss": "CPI",
                        "https://www.bls.gov/feed/empsit.rss": "EmploymentSituation",
                    },
                    clear=True,
                ):
                    from news_archive.collectors.bls_releases import BLSReleasesCollector
                    # Reassign the class attribute to match the patched module-level FEEDS.
                    BLSReleasesCollector.feeds = {
                        "https://www.bls.gov/feed/cpi.rss": "CPI",
                        "https://www.bls.gov/feed/empsit.rss": "EmploymentSituation",
                    }
                    results = list(BLSReleasesCollector().collect())

        assert len(results) == 2
        release_types = set()
        for article, entities in results:
            rt = next(e.entity_value for e in entities if e.entity_type == "release_type")
            release_types.add(rt)
            org = next(e.entity_value for e in entities if e.entity_type == "org")
            assert org == "BLS"
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None

        assert release_types == {"CPI", "EmploymentSituation"}

    def test_one_feed_failure_does_not_kill_run(self) -> None:
        """If one BLS feed 5xx's, the others must still produce articles."""
        def flaky_fetch(url: str) -> bytes:
            if "cpi.rss" in url:
                raise RuntimeError("simulated 503")
            if "empsit.rss" in url:
                return EMPSIT_FIXTURE
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=7):
            with patch(
                "news_archive.collectors.bls_releases.http.fetch_bytes",
                side_effect=flaky_fetch,
            ):
                from news_archive.collectors.bls_releases import BLSReleasesCollector
                BLSReleasesCollector.feeds = {
                    "https://www.bls.gov/feed/cpi.rss": "CPI",
                    "https://www.bls.gov/feed/empsit.rss": "EmploymentSituation",
                }
                results = list(BLSReleasesCollector().collect())

        # cpi failed — we should still have the empsit row.
        assert len(results) == 1
        article, entities = results[0]
        rt = next(e.entity_value for e in entities if e.entity_type == "release_type")
        assert rt == "EmploymentSituation"


class TestParsedTime:
    def test_none_safe(self) -> None:
        assert _parsed_time_to_utc(None) is None

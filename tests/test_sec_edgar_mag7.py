"""Unit tests for the SEC EDGAR Mag 7 collector."""

from __future__ import annotations

import json
from unittest.mock import patch

import feedparser

from news_archive.collectors.sec_edgar_mag7 import (
    FEED_URL_TEMPLATE,
    MAG7,
    _entry_to_dict,
    _extract_form_type,
    _parsed_time_to_utc,
)

AAPL_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Apple Inc.</title>
  <entry>
    <category label="form type" scheme="https://www.sec.gov/" term="8-K" />
    <content type="text/xml">
      <accession-number>0001140361-26-015711</accession-number>
      <filing-date>2026-04-20</filing-date>
      <filing-type>8-K</filing-type>
      <filing-href>https://www.sec.gov/Archives/edgar/data/320193/000114036126015711/</filing-href>
      <form-name>Current report</form-name>
      <file-number>001-36743</file-number>
      <size>239 KB</size>
    </content>
    <id>urn:tag:sec.gov,2008:accession-number=0001140361-26-015711</id>
    <link href="https://www.sec.gov/Archives/edgar/data/320193/000114036126015711/0001140361-26-015711-index.htm" rel="alternate" type="text/html" />
    <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-04-20 &lt;b&gt;AccNo:&lt;/b&gt; 0001140361-26-015711</summary>
    <title>8-K  - Current report</title>
    <updated>2026-04-20T17:29:51-04:00</updated>
  </entry>
  <entry>
    <category label="form type" scheme="https://www.sec.gov/" term="4" />
    <content type="text/xml">
      <accession-number>0001140361-26-015421</accession-number>
      <filing-date>2026-04-17</filing-date>
      <filing-type>4</filing-type>
    </content>
    <id>urn:tag:sec.gov,2008:accession-number=0001140361-26-015421</id>
    <link href="https://www.sec.gov/Archives/edgar/data/320193/000114036126015421/0001140361-26-015421-index.htm" rel="alternate" type="text/html" />
    <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-04-17</summary>
    <title>4  - Statement of changes in beneficial ownership of securities</title>
    <updated>2026-04-17T18:32:12-04:00</updated>
  </entry>
</feed>
""".encode("utf-8")

MSFT_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Microsoft Corporation</title>
  <entry>
    <category label="form type" scheme="https://www.sec.gov/" term="10-Q" />
    <content type="text/xml">
      <accession-number>0000950170-26-009999</accession-number>
      <filing-date>2026-04-15</filing-date>
      <filing-type>10-Q</filing-type>
    </content>
    <id>urn:tag:sec.gov,2008:accession-number=0000950170-26-009999</id>
    <link href="https://www.sec.gov/Archives/edgar/data/789019/000095017026009999/0000950170-26-009999-index.htm" rel="alternate" type="text/html" />
    <summary type="html">&lt;b&gt;Filed:&lt;/b&gt; 2026-04-15</summary>
    <title>10-Q  - Quarterly report</title>
    <updated>2026-04-15T16:05:00-04:00</updated>
  </entry>
</feed>
""".encode("utf-8")


class TestMag7Map:
    def test_all_seven_tickers(self) -> None:
        assert set(MAG7.keys()) == {"AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA"}

    def test_ciks_are_10_digit_strings(self) -> None:
        for ticker, (cik, _company) in MAG7.items():
            assert cik.isdigit(), f"{ticker} CIK not numeric: {cik}"
            assert len(cik) == 10, f"{ticker} CIK not 10 digits: {cik}"

    def test_company_names_non_empty(self) -> None:
        for ticker, (_cik, company) in MAG7.items():
            assert company, f"{ticker} has empty company name"


class TestExtractFormType:
    def test_prefers_filing_type_field(self) -> None:
        feed = feedparser.parse(AAPL_FIXTURE)
        assert _extract_form_type(feed.entries[0]) == "8-K"
        assert _extract_form_type(feed.entries[1]) == "4"

    def test_falls_back_to_tag(self) -> None:
        minimal = """<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>x</id>
    <title>Form Y</title>
    <updated>2026-01-01T00:00:00Z</updated>
    <category label="form type" scheme="https://www.sec.gov/" term="DEF 14A" />
  </entry>
</feed>""".encode("utf-8")
        feed = feedparser.parse(minimal)
        assert _extract_form_type(feed.entries[0]) == "DEF 14A"


class TestParsedTime:
    def test_none_safe(self) -> None:
        assert _parsed_time_to_utc(None) is None


class TestEntryToDict:
    def test_embeds_ticker_and_cik_and_is_json_safe(self) -> None:
        feed = feedparser.parse(AAPL_FIXTURE)
        d = _entry_to_dict(feed.entries[0], ticker="AAPL", cik="0000320193")
        assert d["_ticker"] == "AAPL"
        assert d["_cik"] == "0000320193"
        assert d["accession-number"] == "0001140361-26-015711"
        json.dumps(d)  # must not raise


class TestCollector:
    def test_multi_cik_aggregation_and_entities(self) -> None:
        def fake_fetch(url: str, **_kwargs: object) -> bytes:
            if "CIK=0000320193" in url:
                return AAPL_FIXTURE
            if "CIK=0000789019" in url:
                return MSFT_FIXTURE
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=5):
            with patch(
                "news_archive.collectors.sec_edgar_mag7.http.fetch_bytes",
                side_effect=fake_fetch,
            ):
                # Only exercise two CIKs so we don't sleep the test 6*0.5s.
                with patch(
                    "news_archive.collectors.sec_edgar_mag7.MAG7",
                    {"AAPL": ("0000320193", "Apple Inc."),
                     "MSFT": ("0000789019", "Microsoft Corporation")},
                ):
                    with patch(
                        "news_archive.collectors.sec_edgar_mag7.INTER_FEED_SLEEP_SECONDS",
                        0.0,
                    ):
                        from news_archive.collectors.sec_edgar_mag7 import SECEdgarMag7Collector
                        SECEdgarMag7Collector.tickers = {
                            "AAPL": ("0000320193", "Apple Inc."),
                            "MSFT": ("0000789019", "Microsoft Corporation"),
                        }
                        results = list(SECEdgarMag7Collector().collect())

        # AAPL fixture has 2 entries, MSFT has 1 — total 3.
        assert len(results) == 3

        by_ticker: dict[str, list] = {}
        for article, ents in results:
            ticker = next(e.entity_value for e in ents if e.entity_type == "ticker")
            by_ticker.setdefault(ticker, []).append((article, ents))

        assert set(by_ticker.keys()) == {"AAPL", "MSFT"}
        assert len(by_ticker["AAPL"]) == 2
        assert len(by_ticker["MSFT"]) == 1

        # Check the AAPL 8-K specifically
        aapl_forms = {
            next(e.entity_value for e in ents if e.entity_type == "release_type")
            for _a, ents in by_ticker["AAPL"]
        }
        assert aapl_forms == {"8-K", "4"}

        # Every article has all required timestamps + ticker/org/event entities
        for article, ents in results:
            kinds = {e.entity_type for e in ents}
            assert {"ticker", "org", "event"} <= kinds
            assert next(e.entity_value for e in ents if e.entity_type == "event") == "SECFiling"
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            assert article.external_id  # accession number must be populated

    def test_one_cik_failure_does_not_kill_run(self) -> None:
        def flaky_fetch(url: str, **_kwargs: object) -> bytes:
            if "CIK=0000320193" in url:
                raise RuntimeError("simulated 503")
            if "CIK=0000789019" in url:
                return MSFT_FIXTURE
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=5):
            with patch(
                "news_archive.collectors.sec_edgar_mag7.http.fetch_bytes",
                side_effect=flaky_fetch,
            ):
                with patch(
                    "news_archive.collectors.sec_edgar_mag7.INTER_FEED_SLEEP_SECONDS",
                    0.0,
                ):
                    from news_archive.collectors.sec_edgar_mag7 import SECEdgarMag7Collector
                    SECEdgarMag7Collector.tickers = {
                        "AAPL": ("0000320193", "Apple Inc."),
                        "MSFT": ("0000789019", "Microsoft Corporation"),
                    }
                    results = list(SECEdgarMag7Collector().collect())

        # AAPL failed — MSFT's single entry should still appear.
        assert len(results) == 1
        _, ents = results[0]
        assert next(e.entity_value for e in ents if e.entity_type == "ticker") == "MSFT"


def test_feed_url_template_shape() -> None:
    url = FEED_URL_TEMPLATE.format(cik="0000320193")
    assert "CIK=0000320193" in url
    assert "output=atom" in url

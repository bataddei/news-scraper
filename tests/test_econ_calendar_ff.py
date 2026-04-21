"""Unit tests for the ForexFactory economic calendar collector."""

from __future__ import annotations

import json
from datetime import UTC
from unittest.mock import patch

from news_archive.collectors.econ_calendar_ff import (
    FEED_URL,
    _build_body,
    _extract_slug,
    parse_ff_datetime,
    parse_weekly_events,
)

FIXTURE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<weeklyevents>
  <event>
    <title>CPI m/m</title>
    <country>CAD</country>
    <date><![CDATA[04-20-2026]]></date>
    <time><![CDATA[12:30pm]]></time>
    <impact><![CDATA[High]]></impact>
    <forecast><![CDATA[1.1%]]></forecast>
    <previous><![CDATA[0.5%]]></previous>
    <url><![CDATA[https://www.forexfactory.com/calendar/80-ca-cpi-mm]]></url>
  </event>
  <event>
    <title>Bank Holiday</title>
    <country>JPY</country>
    <date><![CDATA[04-22-2026]]></date>
    <time><![CDATA[All Day]]></time>
    <impact><![CDATA[Holiday]]></impact>
    <forecast />
    <previous />
    <url><![CDATA[https://www.forexfactory.com/calendar/555-jn-bank-holiday]]></url>
  </event>
  <event>
    <title>Tentative Speech</title>
    <country>USD</country>
    <date><![CDATA[04-23-2026]]></date>
    <time><![CDATA[Tentative]]></time>
    <impact><![CDATA[Medium]]></impact>
    <forecast />
    <previous />
    <url><![CDATA[https://www.forexfactory.com/calendar/999-us-fed-speech]]></url>
  </event>
</weeklyevents>
""".encode("utf-8")


class TestSlugExtraction:
    def test_valid_slug(self) -> None:
        assert _extract_slug("https://www.forexfactory.com/calendar/80-ca-cpi-mm") == "80-ca-cpi-mm"

    def test_trailing_slash(self) -> None:
        assert _extract_slug("https://www.forexfactory.com/calendar/777-ch-rate/") == "777-ch-rate"

    def test_none_and_empty(self) -> None:
        assert _extract_slug(None) is None
        assert _extract_slug("") is None

    def test_no_calendar_path(self) -> None:
        assert _extract_slug("https://example.com/other") is None


class TestParseFFDatetime:
    def test_normal_pm_time(self) -> None:
        # 12:30pm ET on 2026-04-20 is 16:30 UTC (EDT is UTC-4 in April).
        dt = parse_ff_datetime("04-20-2026", "12:30pm")
        assert dt is not None
        assert dt.tzinfo == UTC
        assert dt.hour == 16 and dt.minute == 30
        assert dt.year == 2026 and dt.month == 4 and dt.day == 20

    def test_all_day_falls_back_to_midnight_et(self) -> None:
        dt = parse_ff_datetime("04-22-2026", "All Day")
        assert dt is not None
        assert dt.tzinfo == UTC
        # ET midnight 2026-04-22 is 04:00 UTC (EDT offset).
        assert dt.hour == 4 and dt.minute == 0

    def test_tentative_falls_back_to_midnight_et(self) -> None:
        dt = parse_ff_datetime("04-23-2026", "Tentative")
        assert dt is not None
        assert dt.tzinfo == UTC

    def test_empty_time_falls_back_to_midnight_et(self) -> None:
        dt = parse_ff_datetime("04-23-2026", "")
        assert dt is not None
        assert dt.tzinfo == UTC

    def test_invalid_date_returns_none(self) -> None:
        assert parse_ff_datetime("not-a-date", "1:00pm") is None
        assert parse_ff_datetime(None, "1:00pm") is None


class TestParseWeeklyEvents:
    def test_parses_all_three(self) -> None:
        events = parse_weekly_events(FIXTURE_XML)
        assert len(events) == 3
        titles = [e["title"] for e in events]
        assert "CPI m/m" in titles
        assert "Bank Holiday" in titles

    def test_extracts_currency_and_slug(self) -> None:
        events = parse_weekly_events(FIXTURE_XML)
        by_title = {e["title"]: e for e in events}
        assert by_title["CPI m/m"]["currency"] == "CAD"
        assert by_title["CPI m/m"]["slug"] == "80-ca-cpi-mm"

    def test_scheduled_utc_populated_even_for_all_day(self) -> None:
        events = parse_weekly_events(FIXTURE_XML)
        by_title = {e["title"]: e for e in events}
        assert by_title["Bank Holiday"]["scheduled_utc"] is not None
        assert by_title["Tentative Speech"]["scheduled_utc"] is not None


class TestBuildBody:
    def test_all_fields_present(self) -> None:
        body = _build_body(
            {"impact": "High", "forecast": "1.1%", "previous": "0.5%"}
        )
        assert body == "Impact: High | Forecast: 1.1% | Previous: 0.5%"

    def test_missing_fields_omitted(self) -> None:
        body = _build_body({"impact": "High", "forecast": None, "previous": None})
        assert body == "Impact: High"

    def test_empty_returns_none(self) -> None:
        assert _build_body({"impact": None, "forecast": None, "previous": None}) is None


class TestCollector:
    def test_yields_three_articles_with_right_entities(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=12):
            with patch(
                "news_archive.collectors.econ_calendar_ff.http.fetch_bytes",
                return_value=FIXTURE_XML,
            ):
                from news_archive.collectors.econ_calendar_ff import (
                    ForexFactoryCalendarCollector,
                )
                results = list(ForexFactoryCalendarCollector().collect())

        assert len(results) == 3
        for article, ents in results:
            assert article.headline.endswith(")")  # "<title> (<CCY>)"
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            kinds = {e.entity_type for e in ents}
            assert {"event", "org", "release_type"} <= kinds
            assert next(e.entity_value for e in ents if e.entity_type == "event") == "EconCalendar"
            assert next(e.entity_value for e in ents if e.entity_type == "org") == "ForexFactory"
            # raw_payload must be JSON-safe (scheduled_utc is pre-stringified)
            json.dumps(article.raw_payload)

    def test_external_id_uses_slug_and_date(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=12):
            with patch(
                "news_archive.collectors.econ_calendar_ff.http.fetch_bytes",
                return_value=FIXTURE_XML,
            ):
                from news_archive.collectors.econ_calendar_ff import (
                    ForexFactoryCalendarCollector,
                )
                results = list(ForexFactoryCalendarCollector().collect())

        by_title = {a.headline: a for a, _ in results}
        cad = by_title["CPI m/m (CAD)"]
        assert cad.external_id == "80-ca-cpi-mm:04-20-2026"


def test_feed_url_constant() -> None:
    assert FEED_URL.endswith(".xml")
    assert "faireconomy.media" in FEED_URL

"""Unit tests for the Treasury press releases collector."""

from __future__ import annotations

import json
from unittest.mock import patch

from news_archive.collectors.treasury_press import (
    LISTING_URL,
    SECTION_SLUGS,
    _parse_listing,
)

FIXTURE_HTML = """<!doctype html>
<html>
<body>
  <!-- Mega-menu preview (should be ignored — wrong header class) -->
  <div class="mm-news-row">
    <time datetime="2026-04-20T09:00:00Z" class="datetime">April 20, 2026</time>
    <div class="news-title"><a href="/news/press-releases/sb0464">Chair Statement (menu preview)</a></div>
  </div>

  <!-- Main listing -->
  <div class="view-content">
    <div>
      <span class="date-format"><time datetime="2026-04-20T09:00:00Z" class="datetime">April 20, 2026</time></span>
      <span></span>
      <h3 class="featured-stories__headline"><a href="/news/press-releases/sb0464" hreflang="en">Chair's Statement of the United States G20 Presidency</a></h3>
    </div>
    <div>
      <span class="date-format"><time datetime="2026-04-19T18:05:28Z" class="datetime">April 19, 2026</time></span>
      <span>&nbsp;&nbsp;&nbsp;<span class="subcategory"><a href="/news/press-releases/readouts" hreflang="en">Readouts</a></span></span>
      <h3 class="featured-stories__headline"><a href="/news/press-releases/sb0462" hreflang="en">READOUT: Meeting with Queen Maxima</a></h3>
    </div>
    <div>
      <span class="date-format"><time datetime="2026-04-17T18:30:00Z" class="datetime">April 17, 2026</time></span>
      <span></span>
      <h3 class="featured-stories__headline"><a href="/news/press-releases/sb0457" hreflang="en">Treasury Sanctions Recruitment Network Enabling War in Sudan</a></h3>
    </div>

    <!-- Section-index anchor that happens to also live in an h3 — must be skipped -->
    <h3 class="featured-stories__headline"><a href="/news/press-releases/readouts" hreflang="en">Readouts Section</a></h3>
  </div>
</body>
</html>
""".encode("utf-8")


class TestParseListing:
    def test_parses_three_press_releases(self) -> None:
        rows = _parse_listing(FIXTURE_HTML.decode("utf-8"))
        slugs = [r["slug"] for r in rows]
        assert slugs == ["sb0464", "sb0462", "sb0457"]

    def test_skips_section_index_rows(self) -> None:
        rows = _parse_listing(FIXTURE_HTML.decode("utf-8"))
        for r in rows:
            assert r["slug"] not in SECTION_SLUGS

    def test_extracts_subcategory_when_present(self) -> None:
        rows = _parse_listing(FIXTURE_HTML.decode("utf-8"))
        by_slug = {r["slug"]: r for r in rows}
        assert by_slug["sb0462"]["subcategory"] == "Readouts"
        assert by_slug["sb0464"]["subcategory"] is None

    def test_absolute_urls(self) -> None:
        rows = _parse_listing(FIXTURE_HTML.decode("utf-8"))
        for r in rows:
            assert r["url"].startswith("https://home.treasury.gov/news/press-releases/")

    def test_timestamps_are_tz_aware(self) -> None:
        rows = _parse_listing(FIXTURE_HTML.decode("utf-8"))
        for r in rows:
            assert r["published"].tzinfo is not None


class TestCollector:
    def test_collect_yields_articles_with_correct_entities(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=11):
            with patch(
                "news_archive.collectors.treasury_press.http.fetch_bytes",
                return_value=FIXTURE_HTML,
            ):
                from news_archive.collectors.treasury_press import TreasuryPressCollector
                results = list(TreasuryPressCollector().collect())

        assert len(results) == 3
        for article, entities in results:
            kinds = {e.entity_type for e in entities}
            assert "event" in kinds
            assert "org" in kinds
            assert next(e.entity_value for e in entities if e.entity_type == "event") == "TreasuryPress"
            assert next(e.entity_value for e in entities if e.entity_type == "org") == "Treasury"
            assert article.source_published_at.tzinfo is not None
            assert article.source_fetched_at.tzinfo is not None
            assert article.external_id and article.external_id.startswith("sb")
            # raw_payload must be JSON-serializable (datetimes pre-converted to iso)
            json.dumps(article.raw_payload)

    def test_subcategory_emitted_as_release_type(self) -> None:
        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=11):
            with patch(
                "news_archive.collectors.treasury_press.http.fetch_bytes",
                return_value=FIXTURE_HTML,
            ):
                from news_archive.collectors.treasury_press import TreasuryPressCollector
                results = list(TreasuryPressCollector().collect())

        by_id = {a.external_id: ents for a, ents in results}
        rt_for_readout = [e for e in by_id["sb0462"] if e.entity_type == "release_type"]
        assert len(rt_for_readout) == 1
        assert rt_for_readout[0].entity_value == "Readouts"

        # Rows without a subcategory must not produce a release_type entity.
        rt_for_plain = [e for e in by_id["sb0464"] if e.entity_type == "release_type"]
        assert rt_for_plain == []


def test_listing_url_constant() -> None:
    assert LISTING_URL == "https://home.treasury.gov/news/press-releases"

"""Unit tests for the GDELT GKG collector."""

from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import patch

from news_archive.collectors.gdelt_gkg import (
    GKG_COLUMNS,
    LASTUPDATE_URL,
    _iter_gkg_rows,
    extract_mag7_tickers,
    parse_v21_date,
    pick_latest_gkg_url,
    row_passes_filter,
    theme_match,
)

# A plausible 3-line lastupdate.txt. Each line: "<size> <md5> <url>".
LASTUPDATE_BODY = (
    "1234567 abcdef0000000000000000000000000 http://data.gdeltproject.org/gdeltv2/20260421140000.export.CSV.zip\n"
    "2345678 1234567890abcdef0000000000000000 http://data.gdeltproject.org/gdeltv2/20260421140000.mentions.CSV.zip\n"
    "3456789 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa http://data.gdeltproject.org/gdeltv2/20260421140000.gkg.csv.zip\n"
)


def _build_row(**overrides: str) -> str:
    """Compose a tab-separated GKG row from column name → value overrides."""
    values = {c: "" for c in GKG_COLUMNS}
    values.update(overrides)
    return "\t".join(values[c] for c in GKG_COLUMNS)


def _build_csv_bytes(rows: list[str]) -> bytes:
    return ("\n".join(rows) + "\n").encode("utf-8")


def _build_zip_bytes(csv_bytes: bytes, name: str = "20260421140000.gkg.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(name, csv_bytes)
    return buf.getvalue()


class TestPickLatestGkgUrl:
    def test_picks_the_gkg_line(self) -> None:
        url = pick_latest_gkg_url(LASTUPDATE_BODY)
        assert url == "http://data.gdeltproject.org/gdeltv2/20260421140000.gkg.csv.zip"

    def test_returns_none_when_no_gkg(self) -> None:
        no_gkg = "1 a http://x/foo.export.CSV.zip\n2 b http://x/bar.mentions.CSV.zip\n"
        assert pick_latest_gkg_url(no_gkg) is None

    def test_returns_none_for_empty(self) -> None:
        assert pick_latest_gkg_url("") is None


class TestParseV21Date:
    def test_valid(self) -> None:
        dt = parse_v21_date("20260421140000")
        assert dt is not None
        assert (dt.year, dt.month, dt.day) == (2026, 4, 21)
        assert (dt.hour, dt.minute, dt.second) == (14, 0, 0)
        assert dt.tzinfo is not None

    def test_wrong_length(self) -> None:
        assert parse_v21_date("2026") is None

    def test_non_digit(self) -> None:
        assert parse_v21_date("20260421xxxxxx") is None

    def test_none(self) -> None:
        assert parse_v21_date(None) is None

    def test_invalid_month(self) -> None:
        assert parse_v21_date("20261321140000") is None


class TestThemeMatch:
    def test_exact_theme(self) -> None:
        assert theme_match(["FOMC"])
        assert theme_match(["FEDERAL_RESERVE"])

    def test_prefix_theme(self) -> None:
        assert theme_match(["ECON_STOCKMARKET"])
        assert theme_match(["WB_840_LEADING_INDICATORS"])
        assert theme_match(["INFLATION"])

    def test_no_match(self) -> None:
        assert not theme_match(["TAX_ETHNICITY_GERMAN", "SPORTS", "CRIME"])

    def test_empty(self) -> None:
        assert not theme_match([])


class TestExtractMag7Tickers:
    def test_from_url(self) -> None:
        tickers = extract_mag7_tickers("https://apple.com/x", "")
        # "apple" alone isn't in the map; requires "apple inc"
        assert tickers == []

    def test_apple_inc_from_orgs(self) -> None:
        assert extract_mag7_tickers("", "Apple Inc") == ["AAPL"]

    def test_tesla_and_nvidia(self) -> None:
        got = extract_mag7_tickers("https://x.com/tesla-news", "NVIDIA Corporation")
        assert "TSLA" in got and "NVDA" in got

    def test_google_and_alphabet_dedup(self) -> None:
        # Both "google" and "alphabet" map to GOOGL — must not duplicate.
        got = extract_mag7_tickers("https://googleblog.com/x", "Alphabet Inc.")
        assert got == ["GOOGL"]

    def test_none_inputs(self) -> None:
        assert extract_mag7_tickers("", None) == []


class TestRowPassesFilter:
    def test_theme_hit(self) -> None:
        row = {"V1THEMES": "SPORTS;INFLATION;CRIME", "V2DOCUMENTIDENTIFIER": "https://x.com/a"}
        keep, reason = row_passes_filter(row)
        assert keep
        assert reason == "theme"

    def test_mag7_hit_only(self) -> None:
        row = {
            "V1THEMES": "SPORTS;CRIME",
            "V2DOCUMENTIDENTIFIER": "https://x.com/a",
            "V2ENHANCEDORGANIZATIONS": "Nvidia,1234",
        }
        keep, reason = row_passes_filter(row)
        assert keep
        assert reason == "mag7"

    def test_reject(self) -> None:
        row = {
            "V1THEMES": "SPORTS;CRIME;TAX_ETHNICITY_GERMAN",
            "V2DOCUMENTIDENTIFIER": "https://x.com/a",
            "V2ENHANCEDORGANIZATIONS": "Acme Corp",
        }
        keep, reason = row_passes_filter(row)
        assert not keep
        assert reason == ""


class TestIterGkgRows:
    def test_pads_short_rows(self) -> None:
        # Row with fewer columns than GKG_COLUMNS — must be padded, not dropped.
        short = "short-id\t20260421140000\tsrc\tdomain.com"
        rows = list(_iter_gkg_rows(short.encode("utf-8")))
        assert len(rows) == 1
        assert rows[0]["GKGRECORDID"] == "short-id"
        assert rows[0]["V2EXTRASXML"] == ""  # padded

    def test_truncates_long_rows(self) -> None:
        long = "\t".join(["x"] * (len(GKG_COLUMNS) + 5))
        rows = list(_iter_gkg_rows(long.encode("utf-8")))
        assert len(rows) == 1
        assert len(rows[0]) == len(GKG_COLUMNS)

    def test_multiple_rows(self) -> None:
        body = (
            _build_row(GKGRECORDID="A", V1THEMES="INFLATION")
            + "\n"
            + _build_row(GKGRECORDID="B", V1THEMES="SPORTS")
        )
        rows = list(_iter_gkg_rows(body.encode("utf-8")))
        assert [r["GKGRECORDID"] for r in rows] == ["A", "B"]


class TestCollector:
    def test_keeps_filtered_rows_only(self) -> None:
        kept_row = _build_row(
            GKGRECORDID="KEEP-1",
            V21DATE="20260421140000",
            V2SOURCECOMMONNAME="reuters.com",
            V2DOCUMENTIDENTIFIER="https://reuters.com/biz/fed",
            V1THEMES="ECON_INTEREST_RATE;FOMC",
            V2ENHANCEDORGANIZATIONS="Federal Reserve,1234",
            V15TONE="-1.2,3.4,5.6",
        )
        mag7_row = _build_row(
            GKGRECORDID="KEEP-2",
            V21DATE="20260421140000",
            V2SOURCECOMMONNAME="cnbc.com",
            V2DOCUMENTIDENTIFIER="https://cnbc.com/nvidia-stuff",
            V1THEMES="SPORTS",
            V2ENHANCEDORGANIZATIONS="NVIDIA Corporation,1",
        )
        drop_row = _build_row(
            GKGRECORDID="DROP-1",
            V1THEMES="SPORTS;CRIME;TAX_ETHNICITY_GERMAN",
            V2DOCUMENTIDENTIFIER="https://gossip.example/x",
            V2ENHANCEDORGANIZATIONS="Acme",
        )
        csv_bytes = _build_csv_bytes([kept_row, mag7_row, drop_row])
        zip_bytes = _build_zip_bytes(csv_bytes)

        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return LASTUPDATE_BODY.encode("utf-8")
            if url.endswith(".gkg.csv.zip"):
                return zip_bytes
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=7):
            with patch(
                "news_archive.collectors.gdelt_gkg.http.fetch_bytes",
                side_effect=fake_fetch,
            ):
                from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
                results = list(GdeltGkgCollector().collect())

        assert len(results) == 2
        by_ext = {a.external_id: (a, ents) for a, ents in results}
        assert "KEEP-1" in by_ext and "KEEP-2" in by_ext

        keep1_article, keep1_ents = by_ext["KEEP-1"]
        assert keep1_article.source_published_at.tzinfo is not None
        assert keep1_article.source_fetched_at.tzinfo is not None
        kinds1 = {e.entity_type for e in keep1_ents}
        assert {"event", "org"} <= kinds1
        assert next(e.entity_value for e in keep1_ents if e.entity_type == "event") == "GDELT"
        # raw_payload must be JSON-safe
        json.dumps(keep1_article.raw_payload)

        _keep2_article, keep2_ents = by_ext["KEEP-2"]
        tickers = [e.entity_value for e in keep2_ents if e.entity_type == "ticker"]
        assert "NVDA" in tickers

    def test_no_gkg_url_noop(self) -> None:
        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return b"1 a http://x/foo.export.CSV.zip\n"
            raise AssertionError(f"unexpected url {url}")

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=7):
            with patch(
                "news_archive.collectors.gdelt_gkg.http.fetch_bytes",
                side_effect=fake_fetch,
            ):
                from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
                results = list(GdeltGkgCollector().collect())

        assert results == []

    def test_bad_zip_noop(self) -> None:
        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return LASTUPDATE_BODY.encode("utf-8")
            return b"not a zip"

        with patch("news_archive.collectors.base.db.get_source_id_by_slug", return_value=7):
            with patch(
                "news_archive.collectors.gdelt_gkg.http.fetch_bytes",
                side_effect=fake_fetch,
            ):
                from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
                results = list(GdeltGkgCollector().collect())

        assert results == []


def test_lastupdate_url_constant() -> None:
    assert LASTUPDATE_URL.endswith("lastupdate.txt")

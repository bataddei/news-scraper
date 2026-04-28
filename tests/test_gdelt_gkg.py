"""Unit tests for the GDELT GKG collector (rollup mode)."""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from unittest.mock import patch

from news_archive.collectors.gdelt_gkg import (
    GKG_COLUMNS,
    LASTUPDATE_URL,
    _iter_gkg_rows,
    buckets_for_row,
    compute_rollups,
    extract_mag7_tickers,
    parse_overall_tone,
    parse_v21_date,
    parse_window_start_from_url,
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


class TestParseWindowStartFromUrl:
    def test_valid_url(self) -> None:
        dt = parse_window_start_from_url(
            "http://data.gdeltproject.org/gdeltv2/20260421140000.gkg.csv.zip"
        )
        assert dt == datetime(2026, 4, 21, 14, 0, 0, tzinfo=UTC)

    def test_unparseable_returns_none(self) -> None:
        assert parse_window_start_from_url("http://x/garbage.csv.zip") is None


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


class TestBucketsForRow:
    def test_fomc_bucket_from_exact_theme(self) -> None:
        assert "FOMC" in buckets_for_row(["FOMC"], [])
        assert "FOMC" in buckets_for_row(["FEDERAL_RESERVE"], [])

    def test_inflation_and_interest_rate(self) -> None:
        b = buckets_for_row(["INFLATION", "INTEREST_RATE_CUT"], [])
        assert {"INFLATION", "INTEREST_RATE"} <= b

    def test_econ_catchall(self) -> None:
        b = buckets_for_row(["ECON_STOCKMARKET"], [])
        assert "ECON" in b

    def test_employment_covers_un_and_employed(self) -> None:
        assert "EMPLOYMENT" in buckets_for_row(["UNEMPLOYMENT_RATE"], [])
        assert "EMPLOYMENT" in buckets_for_row(["EMPLOYMENT_PAYROLLS"], [])

    def test_mag7_buckets_per_ticker(self) -> None:
        b = buckets_for_row([], ["AAPL", "TSLA"])
        assert {"MAG7_AAPL", "MAG7_TSLA"} <= b

    def test_combined(self) -> None:
        b = buckets_for_row(["FOMC", "ECON_STOCKMARKET"], ["AAPL"])
        assert {"FOMC", "ECON", "MAG7_AAPL"} <= b

    def test_empty(self) -> None:
        assert buckets_for_row([], []) == set()


class TestParseOverallTone:
    def test_first_value_used(self) -> None:
        assert parse_overall_tone("-1.2,3.4,5.6,2.1,7.0,0,42") == -1.2

    def test_empty_returns_none(self) -> None:
        assert parse_overall_tone("") is None
        assert parse_overall_tone(None) is None

    def test_unparseable_returns_none(self) -> None:
        assert parse_overall_tone("nope") is None


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


class TestComputeRollups:
    """The aggregation step: rows in → rollup rows out."""

    WINDOW = datetime(2026, 4, 21, 14, 0, 0, tzinfo=UTC)
    FETCHED = datetime(2026, 4, 21, 14, 1, 30, tzinfo=UTC)

    def _row(self, **overrides: str) -> dict[str, str]:
        return {col: overrides.get(col, "") for col in GKG_COLUMNS}

    def test_fomc_rollup_aggregates_three_rows(self) -> None:
        rows = [
            self._row(
                V2SOURCECOMMONNAME="reuters.com",
                V2DOCUMENTIDENTIFIER="https://reuters.com/a",
                V1THEMES="FOMC",
                V15TONE="-2.0,1,3,2,5,0,40",
            ),
            self._row(
                V2SOURCECOMMONNAME="reuters.com",
                V2DOCUMENTIDENTIFIER="https://reuters.com/b",
                V1THEMES="FOMC;INFLATION",
                V15TONE="0.0,2,2,4,5,0,40",
            ),
            self._row(
                V2SOURCECOMMONNAME="cnbc.com",
                V2DOCUMENTIDENTIFIER="https://cnbc.com/c",
                V1THEMES="FEDERAL_RESERVE",
                V15TONE="2.0,3,1,4,5,0,40",
            ),
        ]
        rollups, total, kept = compute_rollups(
            iter(rows), window_start=self.WINDOW, fetched_at=self.FETCHED
        )
        assert total == 3
        assert kept == 3

        by_bucket = {r.theme_bucket: r for r in rollups}
        assert "FOMC" in by_bucket
        fomc = by_bucket["FOMC"]
        assert fomc.n_articles == 3
        assert fomc.n_sources == 2  # reuters.com + cnbc.com
        assert fomc.window_start == self.WINDOW
        assert fomc.fetched_at == self.FETCHED
        assert fomc.avg_tone == 0.0  # (-2 + 0 + 2) / 3
        assert fomc.min_tone == -2.0
        assert fomc.max_tone == 2.0
        assert fomc.top_domain == "reuters.com"  # most common
        assert fomc.top_url == "https://reuters.com/a"  # first kept row's URL

        # The INFLATION bucket should also pick up row b.
        assert "INFLATION" in by_bucket
        assert by_bucket["INFLATION"].n_articles == 1

    def test_drops_non_matching_rows(self) -> None:
        rows = [
            self._row(
                V2SOURCECOMMONNAME="gossip.example",
                V2DOCUMENTIDENTIFIER="https://gossip.example/x",
                V1THEMES="SPORTS;CRIME;TAX_ETHNICITY_GERMAN",
            ),
            self._row(
                V2SOURCECOMMONNAME="reuters.com",
                V2DOCUMENTIDENTIFIER="https://reuters.com/fed",
                V1THEMES="FOMC",
            ),
        ]
        rollups, total, kept = compute_rollups(
            iter(rows), window_start=self.WINDOW, fetched_at=self.FETCHED
        )
        assert total == 2
        assert kept == 1
        assert {r.theme_bucket for r in rollups} == {"FOMC"}

    def test_mag7_creates_per_ticker_bucket(self) -> None:
        rows = [
            self._row(
                V2SOURCECOMMONNAME="cnbc.com",
                V2DOCUMENTIDENTIFIER="https://cnbc.com/nvda",
                V1THEMES="SPORTS",  # only mag7 path matches
                V2ENHANCEDORGANIZATIONS="NVIDIA Corporation,1",
            ),
            self._row(
                V2SOURCECOMMONNAME="bloomberg.com",
                V2DOCUMENTIDENTIFIER="https://bloomberg.com/aapl",
                V1THEMES="SPORTS",
                V2ENHANCEDORGANIZATIONS="Apple Inc,1",
            ),
        ]
        rollups, total, kept = compute_rollups(
            iter(rows), window_start=self.WINDOW, fetched_at=self.FETCHED
        )
        assert total == 2
        assert kept == 2
        buckets = {r.theme_bucket: r for r in rollups}
        assert "MAG7_NVDA" in buckets
        assert "MAG7_AAPL" in buckets
        assert buckets["MAG7_NVDA"].n_articles == 1
        assert buckets["MAG7_AAPL"].n_articles == 1

    def test_tone_is_optional(self) -> None:
        rows = [
            self._row(
                V2SOURCECOMMONNAME="x.com",
                V2DOCUMENTIDENTIFIER="https://x.com/a",
                V1THEMES="FOMC",
                V15TONE="",  # missing tone
            ),
        ]
        rollups, _, _ = compute_rollups(
            iter(rows), window_start=self.WINDOW, fetched_at=self.FETCHED
        )
        fomc = next(r for r in rollups if r.theme_bucket == "FOMC")
        assert fomc.avg_tone is None
        assert fomc.min_tone is None
        assert fomc.max_tone is None

    def test_empty_input_yields_no_rollups(self) -> None:
        rollups, total, kept = compute_rollups(
            iter(()), window_start=self.WINDOW, fetched_at=self.FETCHED
        )
        assert rollups == []
        assert total == 0
        assert kept == 0


class TestCollectorRun:
    """Exercise GdeltGkgCollector.run() end-to-end with HTTP and DB stubbed.

    Patches reach `db` via `news_archive.collectors.gdelt_gkg.db` (the local
    binding inside the collector module).
    """

    def _patch_collector(self, fake_fetch, captured: list | None = None):
        """Build the stack of context managers the collector's run() needs."""
        from contextlib import ExitStack

        def fake_insert(rollups):
            if captured is not None:
                captured.extend(rollups)
            return len(rollups), 0

        stack = ExitStack()
        stack.enter_context(patch(
            "news_archive.collectors.base.db.get_source_id_by_slug", return_value=7
        ))
        stack.enter_context(patch(
            "news_archive.collectors.gdelt_gkg.http.fetch_bytes",
            side_effect=fake_fetch,
        ))
        stack.enter_context(patch(
            "news_archive.collectors.gdelt_gkg.db.start_collection_run", return_value=1
        ))
        stack.enter_context(patch(
            "news_archive.collectors.gdelt_gkg.db.finish_collection_run"
        ))
        ins = stack.enter_context(patch(
            "news_archive.collectors.gdelt_gkg.db.insert_gdelt_rollups",
            side_effect=fake_insert,
        ))
        return stack, ins

    def test_emits_rollups_via_db(self) -> None:
        kept = _build_row(
            GKGRECORDID="K-1",
            V21DATE="20260421140000",
            V2SOURCECOMMONNAME="reuters.com",
            V2DOCUMENTIDENTIFIER="https://reuters.com/biz/fed",
            V1THEMES="FOMC;INFLATION",
            V15TONE="-1.5,2.0,3.5,1.0,5.0,0,40",
        )
        mag7 = _build_row(
            GKGRECORDID="K-2",
            V21DATE="20260421140000",
            V2SOURCECOMMONNAME="cnbc.com",
            V2DOCUMENTIDENTIFIER="https://cnbc.com/nvidia",
            V1THEMES="SPORTS",
            V2ENHANCEDORGANIZATIONS="NVIDIA Corporation,1",
        )
        drop = _build_row(
            GKGRECORDID="D-1",
            V1THEMES="SPORTS;CRIME",
            V2DOCUMENTIDENTIFIER="https://gossip.example/x",
        )
        zip_bytes = _build_zip_bytes(_build_csv_bytes([kept, mag7, drop]))

        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return LASTUPDATE_BODY.encode("utf-8")
            if url.endswith(".gkg.csv.zip"):
                return zip_bytes
            raise AssertionError(f"unexpected url {url}")

        captured: list = []
        stack, _ins = self._patch_collector(fake_fetch, captured=captured)
        with stack:
            from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
            run = GdeltGkgCollector().run()

        assert run.status == "success"
        by_bucket = {r.theme_bucket: r for r in captured}
        assert {"FOMC", "INFLATION", "MAG7_NVDA"} <= set(by_bucket)
        assert "SPORTS" not in by_bucket  # drop row never produced a bucket

        fomc = by_bucket["FOMC"]
        assert fomc.window_start == datetime(2026, 4, 21, 14, 0, 0, tzinfo=UTC)
        assert fomc.fetched_at.tzinfo is not None
        assert fomc.n_articles == 1
        assert fomc.top_url == "https://reuters.com/biz/fed"
        assert fomc.avg_tone == -1.5

    def test_no_gkg_url_is_successful_noop(self) -> None:
        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return b"1 a http://x/foo.export.CSV.zip\n"
            raise AssertionError(f"unexpected url {url}")

        stack, ins = self._patch_collector(fake_fetch)
        with stack:
            from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
            run = GdeltGkgCollector().run()
        assert run.status == "success"
        assert run.articles_inserted == 0
        ins.assert_not_called()

    def test_bad_zip_is_successful_noop(self) -> None:
        def fake_fetch(url: str, **_: object) -> bytes:
            if url == LASTUPDATE_URL:
                return LASTUPDATE_BODY.encode("utf-8")
            return b"not a zip"

        stack, ins = self._patch_collector(fake_fetch)
        with stack:
            from news_archive.collectors.gdelt_gkg import GdeltGkgCollector
            run = GdeltGkgCollector().run()
        assert run.status == "success"
        ins.assert_not_called()


def test_lastupdate_url_constant() -> None:
    assert LASTUPDATE_URL.endswith("lastupdate.txt")

"""Unit tests for gap detection.

Covers the pure `find_gaps` function only — the DB query is a straight
LEFT JOIN and is exercised via the CLI during live verification.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from news_archive.monitoring.gaps import (
    SOURCE_MAX_GAP_SECONDS,
    Gap,
    find_gaps,
    format_gap,
)

NOW = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)


def _row(slug: str, last: datetime | None, sid: int = 1) -> dict:
    return {"id": sid, "slug": slug, "last_success_at": last}


class TestFindGaps:
    def test_fresh_source_not_flagged(self) -> None:
        rows = [_row("fed_speeches", NOW - timedelta(minutes=30))]
        assert find_gaps(rows, now=NOW) == []

    def test_never_run_flagged(self) -> None:
        rows = [_row("fed_speeches", None, sid=3)]
        gaps = find_gaps(rows, now=NOW)
        assert len(gaps) == 1
        assert gaps[0].kind == "never_run"
        assert gaps[0].source_slug == "fed_speeches"
        assert gaps[0].source_id == 3
        assert gaps[0].last_success_at is None
        assert gaps[0].seconds_since_last is None

    def test_overdue_flagged(self) -> None:
        # hourly cadence, tolerance 2.5h → a 3h gap is overdue
        rows = [_row("fed_speeches", NOW - timedelta(hours=3))]
        gaps = find_gaps(rows, now=NOW)
        assert len(gaps) == 1
        g = gaps[0]
        assert g.kind == "overdue"
        assert g.seconds_since_last == 3 * 3600
        assert g.max_gap_seconds == SOURCE_MAX_GAP_SECONDS["fed_speeches"]

    def test_exactly_at_tolerance_not_flagged(self) -> None:
        # delta == max_gap → treated as not overdue (strict >)
        max_gap = SOURCE_MAX_GAP_SECONDS["fed_speeches"]
        rows = [_row("fed_speeches", NOW - timedelta(seconds=max_gap))]
        assert find_gaps(rows, now=NOW) == []

    def test_unknown_slug_ignored(self) -> None:
        # Source exists in DB but has no tolerance entry — don't alarm.
        rows = [_row("deprecated_feed", NOW - timedelta(days=30))]
        assert find_gaps(rows, now=NOW) == []

    def test_multiple_sources_mixed(self) -> None:
        rows = [
            _row("fed_speeches", NOW - timedelta(minutes=5), sid=1),          # OK
            _row("sec_edgar_mag7", NOW - timedelta(hours=2), sid=2),          # OVERDUE (45m tol)
            _row("treasury_press", None, sid=3),                              # NEVER_RUN
            _row("gdelt_gkg", NOW - timedelta(minutes=40), sid=4),            # OK (150m tol)
            _row("some_other_source", NOW - timedelta(days=30), sid=99),      # unknown, ignored
        ]
        gaps = find_gaps(rows, now=NOW)
        slugs_by_kind = {(g.source_slug, g.kind) for g in gaps}
        assert slugs_by_kind == {
            ("sec_edgar_mag7", "overdue"),
            ("treasury_press", "never_run"),
        }

    def test_custom_tolerances_override(self) -> None:
        rows = [_row("fed_speeches", NOW - timedelta(minutes=10))]
        # Very tight tolerance (5 minutes) — should flag the 10-minute-old row
        gaps = find_gaps(rows, now=NOW, tolerances={"fed_speeches": 300})
        assert len(gaps) == 1
        assert gaps[0].kind == "overdue"


class TestSourceMaxGapSeconds:
    def test_covers_all_registered_collectors(self) -> None:
        # If we add a new collector, we want the gap check to include it by
        # default. Keep this map in sync with the COLLECTORS dict.
        from news_archive.collectors.run import COLLECTORS
        missing = set(COLLECTORS.keys()) - set(SOURCE_MAX_GAP_SECONDS.keys())
        assert not missing, f"collectors without gap tolerance: {sorted(missing)}"

    def test_all_tolerances_positive(self) -> None:
        for slug, secs in SOURCE_MAX_GAP_SECONDS.items():
            assert secs > 0, f"{slug} has non-positive tolerance: {secs}"


class TestFormatGap:
    def test_never_run_message(self) -> None:
        gap = Gap(
            source_slug="bls_releases",
            source_id=4,
            kind="never_run",
            last_success_at=None,
            seconds_since_last=None,
            max_gap_seconds=9000,
        )
        line = format_gap(gap)
        assert "bls_releases" in line
        assert "NEVER_RUN" in line

    def test_overdue_message(self) -> None:
        last = datetime(2026, 4, 22, 9, 0, 0, tzinfo=UTC)
        gap = Gap(
            source_slug="fed_speeches",
            source_id=2,
            kind="overdue",
            last_success_at=last,
            seconds_since_last=3 * 3600,
            max_gap_seconds=9000,
        )
        line = format_gap(gap)
        assert "fed_speeches" in line
        assert "OVERDUE" in line
        assert "180m ago" in line
        assert "tolerance 150m" in line
        assert "2026-04-22T09:00:00" in line

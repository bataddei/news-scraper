"""Gap detection — "which sources haven't reported in too long?".

For each active source we know its cron cadence. If the most recent non-failed
`collection_runs` row is older than a per-source tolerance, the source is a
gap. Two gap kinds:

    * `never_run` — the source has zero non-failed runs on record. Usually
      means the collector was seeded but never wired up / never fired.
    * `overdue` — there is a last-success timestamp, but it's older than
      the tolerance window.

A run counts as "non-failed" if its status is `success` or `partial` and
`finished_at` is populated (we ignore `running` rows — those haven't
committed yet and may be in-flight or crashed).

The tolerance is deliberately ~2× the cron interval. That gives a single
missed tick worth of slack before we alert, which matches our preference
for quiet-until-something-is-actually-wrong monitoring.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

# Per-source max tolerated gap between successful runs, in seconds.
# Keep in sync with `deploy/cron/news-pipeline.cron`. Values are roughly
# 2× the cron cadence so a single missed tick doesn't trigger an alert.
SOURCE_MAX_GAP_SECONDS: dict[str, int] = {
    "fed_fomc_statements": 5 * 3600,      # cron: every 4h
    "fed_speeches":        2 * 3600 + 1800,  # cron: hourly (2.5h tolerance)
    "bls_releases":        2 * 3600 + 1800,  # cron: hourly
    "treasury_press":      2 * 3600 + 1800,  # cron: hourly
    "sec_edgar_mag7":      45 * 60,          # cron: */15
    "wire_pr_newswire":    2 * 3600 + 1800,  # cron: hourly
    "wire_globenewswire":  2 * 3600 + 1800,  # cron: hourly
    "econ_calendar_ff":    14 * 3600,        # cron: twice daily
    "gdelt_gkg":           2 * 3600 + 1800,  # cron: hourly
}


@dataclass(frozen=True)
class Gap:
    source_slug: str
    source_id: int
    kind: str  # "never_run" | "overdue"
    last_success_at: datetime | None
    seconds_since_last: int | None
    max_gap_seconds: int


def find_gaps(
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    tolerances: dict[str, int] | None = None,
) -> list[Gap]:
    """Pure function: given `(source_id, slug, last_success_at)` rows + a clock, return gaps.

    `rows` must contain one row per source present in the `sources` table
    (LEFT JOIN so sources with no runs yet still appear, with `last_success_at` NULL).
    Sources not listed in `tolerances` are skipped — we don't have an SLO
    for them. Unknown-slug sources are therefore silently ignored rather
    than alarming, by design.
    """
    tol = tolerances if tolerances is not None else SOURCE_MAX_GAP_SECONDS
    gaps: list[Gap] = []
    for row in rows:
        slug = row["slug"]
        if slug not in tol:
            continue
        max_gap = tol[slug]
        last: datetime | None = row.get("last_success_at")
        if last is None:
            gaps.append(
                Gap(
                    source_slug=slug,
                    source_id=int(row["id"]),
                    kind="never_run",
                    last_success_at=None,
                    seconds_since_last=None,
                    max_gap_seconds=max_gap,
                )
            )
            continue
        delta_seconds = int((now - last).total_seconds())
        if delta_seconds > max_gap:
            gaps.append(
                Gap(
                    source_slug=slug,
                    source_id=int(row["id"]),
                    kind="overdue",
                    last_success_at=last,
                    seconds_since_last=delta_seconds,
                    max_gap_seconds=max_gap,
                )
            )
    return gaps


def fetch_latest_success_per_source(
    conn: psycopg.Connection,
) -> list[dict[str, Any]]:
    """DB I/O: last success/partial run per source.

    LEFT JOIN so that sources with zero non-failed runs still appear with
    NULL last_success_at — `find_gaps` flags those as `never_run`.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                s.id,
                s.slug,
                max(cr.finished_at) filter (
                    where cr.status in ('success', 'partial')
                      and cr.finished_at is not null
                ) as last_success_at
            from news_archive.sources s
            left join news_archive.collection_runs cr on cr.source_id = s.id
            group by s.id, s.slug
            order by s.slug
            """
        )
        return list(cur.fetchall())


def check_gaps_now(conn: psycopg.Connection) -> list[Gap]:
    """End-to-end: fetch state from DB, apply the pure rule. Convenience wrapper."""
    rows = fetch_latest_success_per_source(conn)
    return find_gaps(rows, now=datetime.now(UTC))


def format_gap(gap: Gap) -> str:
    """Human-readable one-line gap description, suitable for logs or chat."""
    if gap.kind == "never_run":
        return (
            f"[{gap.source_slug}] NEVER_RUN — no successful collection_runs row "
            f"(tolerance {gap.max_gap_seconds // 60}m)"
        )
    assert gap.last_success_at is not None
    assert gap.seconds_since_last is not None
    mins_since = gap.seconds_since_last // 60
    mins_tol = gap.max_gap_seconds // 60
    return (
        f"[{gap.source_slug}] OVERDUE — last success "
        f"{gap.last_success_at.isoformat(timespec='seconds')} "
        f"({mins_since}m ago, tolerance {mins_tol}m)"
    )

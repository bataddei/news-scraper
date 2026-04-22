"""Daily integrity report — aggregate Supabase + droplet state into a chat message.

Content per brief §Week 4:
  * Articles collected per source in last 24h (seen / inserted / duplicate)
  * Failed runs in last 24h
  * Overall article count and DB size
  * Droplet disk usage
  * Active gaps (from monitoring.gaps)

Format is HTML, which Telegram accepts out of the box and renders with
a monospace block via <pre>. HTML escaping is cheaper and less fraught
than MarkdownV2's dot/paren escape rules.

Split into three layers so the formatter can be unit-tested without DB:
  * `fetch_*` functions do I/O,
  * `ReportData` holds the pure snapshot,
  * `format_report` turns a snapshot into a string.
"""

from __future__ import annotations

import html
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg

from news_archive.monitoring.gaps import Gap


@dataclass(frozen=True)
class SourceRow:
    slug: str
    success_runs: int
    partial_runs: int
    failed_runs: int
    articles_seen: int
    articles_inserted: int
    articles_duplicate: int


@dataclass(frozen=True)
class DiskUsage:
    total_bytes: int
    used_bytes: int
    free_bytes: int

    @property
    def percent_used(self) -> float:
        return 100.0 * self.used_bytes / self.total_bytes if self.total_bytes else 0.0


@dataclass(frozen=True)
class ReportData:
    generated_at: datetime
    per_source: list[SourceRow]
    total_articles: int
    db_size_pretty: str
    disk: DiskUsage
    gaps: list[Gap] = field(default_factory=list)


# --- I/O layer ---------------------------------------------------------------


def fetch_per_source_last_24h(
    conn: psycopg.Connection,
    *,
    only_slugs: list[str] | None = None,
) -> list[SourceRow]:
    """One row per source with counts for the last 24 hours.

    If `only_slugs` is provided, rows for other sources are omitted. This
    keeps the daily report focused on actively-registered collectors and
    skips seeded-but-dead Tier-2 sources (Reuters, AP, Business Wire).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                s.slug,
                count(*) filter (where cr.status = 'success') as success_runs,
                count(*) filter (where cr.status = 'partial') as partial_runs,
                count(*) filter (where cr.status = 'failed')  as failed_runs,
                coalesce(sum(cr.articles_seen), 0)      as articles_seen,
                coalesce(sum(cr.articles_inserted), 0)  as articles_inserted,
                coalesce(sum(cr.articles_duplicate), 0) as articles_duplicate
            from news_archive.sources s
            left join news_archive.collection_runs cr
                   on cr.source_id = s.id
                  and cr.finished_at > now() - interval '24 hours'
            where %(only_slugs)s::text[] is null or s.slug = any(%(only_slugs)s::text[])
            group by s.slug
            order by s.slug
            """,
            {"only_slugs": only_slugs},
        )
        rows = cur.fetchall()
    return [
        SourceRow(
            slug=r["slug"],
            success_runs=int(r["success_runs"]),
            partial_runs=int(r["partial_runs"]),
            failed_runs=int(r["failed_runs"]),
            articles_seen=int(r["articles_seen"]),
            articles_inserted=int(r["articles_inserted"]),
            articles_duplicate=int(r["articles_duplicate"]),
        )
        for r in rows
    ]


def fetch_total_articles(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("select count(*) as n from news_archive.articles")
        row = cur.fetchone()
        assert row is not None
        return int(row["n"])


def fetch_db_size_pretty(conn: psycopg.Connection) -> str:
    """Human-readable total database size (Supabase-side)."""
    with conn.cursor() as cur:
        cur.execute("select pg_size_pretty(pg_database_size(current_database())) as s")
        row = cur.fetchone()
        assert row is not None
        return str(row["s"])


def gather_disk_usage(path: str = "/") -> DiskUsage:
    """Droplet-local disk stats. Uses `/` since our filesystem is one partition."""
    total, used, free = shutil.disk_usage(path)
    return DiskUsage(total_bytes=total, used_bytes=used, free_bytes=free)


def fetch_report_data(
    conn: psycopg.Connection,
    *,
    generated_at: datetime,
    only_slugs: list[str] | None = None,
) -> ReportData:
    """Assemble everything except gaps (caller passes those separately)."""
    return ReportData(
        generated_at=generated_at,
        per_source=fetch_per_source_last_24h(conn, only_slugs=only_slugs),
        total_articles=fetch_total_articles(conn),
        db_size_pretty=fetch_db_size_pretty(conn),
        disk=gather_disk_usage(),
        gaps=[],
    )


# --- Formatting layer --------------------------------------------------------


def _bytes_pretty(n: int) -> str:
    """1234567 -> '1.2 MB'. Keep it short — Telegram messages are narrow."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n //= 1024
    return f"{n} TB"


def _format_per_source_table(rows: list[SourceRow]) -> str:
    """Fixed-width table for <pre> block. Truncates the slug column if needed."""
    if not rows:
        return "(no sources)"
    slug_w = min(max(len(r.slug) for r in rows), 22)
    header = f"{'source':<{slug_w}} {'ins':>5} {'dup':>5} {'seen':>5} {'fail':>4}"
    lines = [header, "-" * len(header)]
    for r in rows:
        slug = r.slug if len(r.slug) <= slug_w else r.slug[: slug_w - 1] + "…"
        lines.append(
            f"{slug:<{slug_w}} "
            f"{r.articles_inserted:>5} "
            f"{r.articles_duplicate:>5} "
            f"{r.articles_seen:>5} "
            f"{r.failed_runs:>4}"
        )
    return "\n".join(lines)


def _format_gaps_section(gaps: list[Gap]) -> str:
    if not gaps:
        return "✅ No gaps."
    lines = []
    for g in gaps:
        if g.kind == "never_run":
            lines.append(f"⚠️ <b>{html.escape(g.source_slug)}</b> never produced a successful run")
        else:
            assert g.seconds_since_last is not None
            mins = g.seconds_since_last // 60
            lines.append(
                f"⚠️ <b>{html.escape(g.source_slug)}</b> overdue — last success {mins}m ago"
            )
    return "\n".join(lines)


def format_report(data: ReportData) -> str:
    """Render `ReportData` as a Telegram-friendly HTML message."""
    generated = data.generated_at.strftime("%Y-%m-%d %H:%M UTC")
    table = _format_per_source_table(data.per_source)

    total_inserted = sum(r.articles_inserted for r in data.per_source)
    total_failed = sum(r.failed_runs for r in data.per_source)

    parts = [
        "📊 <b>News Archive — Daily Report</b>",
        f"<i>{html.escape(generated)}</i>",
        "",
        f"<b>Last 24h:</b> {total_inserted:,} inserted, {total_failed} failed runs",
        f"<pre>{html.escape(table)}</pre>",
        "",
        "<b>Archive</b>",
        f"• Total articles: {data.total_articles:,}",
        f"• DB size: {html.escape(data.db_size_pretty)}",
        f"• Droplet disk: {data.disk.percent_used:.0f}% used "
        f"({_bytes_pretty(data.disk.used_bytes)} / {_bytes_pretty(data.disk.total_bytes)})",
        "",
        "<b>Gaps</b>",
        _format_gaps_section(data.gaps),
    ]
    return "\n".join(parts)


# --- Telegram transport ------------------------------------------------------


TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def send_telegram(
    text: str,
    *,
    bot_token: str,
    chat_id: str,
    parse_mode: str = "HTML",
) -> dict[str, Any]:
    """POST to Telegram's sendMessage. Raises on HTTP or API error.

    Returns the parsed JSON response (handy for tests / debugging).
    Split from report generation so tests can unit-test formatting
    without needing a bot token.
    """
    import httpx

    url = TELEGRAM_API.format(token=bot_token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()
    if not body.get("ok"):
        raise RuntimeError(f"telegram sendMessage failed: {body}")
    return body

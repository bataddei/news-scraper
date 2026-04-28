"""One-shot: backfill rollups from existing GDELT articles, then delete them.

Stages (each idempotent; use flags to run subsets):

    --rollup    Aggregate every existing news_archive.articles row from
                source=gdelt_gkg into news_archive.gdelt_rollup_15min.
                ON CONFLICT DO NOTHING — re-runs are no-ops once a window's
                buckets exist.
    --delete    DELETE the GDELT article rows (cascades to article_entities).
                Batched to keep WAL bounded.
    --vacuum    VACUUM FULL articles + article_entities to reclaim TOAST.
    --all       Run rollup → delete → vacuum in sequence.

Why this is safe:
  * Rollup logic (buckets_for_row, _BucketAcc) is the SAME code path the live
    collector now uses, so historical rollups are computed identically to new ones.
  * MAG7 ticker assignment for historical rows uses the article_entities
    table (ticker entries written at original ingest time), since the slim
    raw_payload no longer carries V2ENHANCEDORGANIZATIONS.
  * Per-article data dropped here is recoverable by re-downloading the GKG
    file at raw_payload->>'_gdelt_file', though that's a chore.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime

import psycopg

from news_archive.collectors.gdelt_gkg import (
    _BucketAcc,
    buckets_for_row,
    parse_overall_tone,
)
from news_archive.config import settings
from news_archive.db import (
    connection,
    get_source_id_by_slug,
    insert_gdelt_rollups,
)
from news_archive.models import GdeltRollup

GDELT_FILE_TS_RE = re.compile(r"(\d{14})")


def _window_start_from_file_url(file_url: str | None) -> datetime | None:
    """Extract the YYYYMMDDHHMMSS stem from the GDELT file URL stored in raw_payload."""
    if not file_url:
        return None
    m = GDELT_FILE_TS_RE.search(file_url)
    if not m:
        return None
    s = m.group(1)
    try:
        return datetime(
            int(s[0:4]), int(s[4:6]), int(s[6:8]),
            int(s[8:10]), int(s[10:12]), int(s[12:14]),
            tzinfo=UTC,
        )
    except ValueError:
        return None


def _themes_from_cell(cell: str | None) -> list[str]:
    if not cell:
        return []
    return [t.strip() for t in cell.split(";") if t.strip()]


def stage_rollup(source_id: int, batch_size: int = 10_000) -> None:
    """Page through every GDELT article + its ticker entities, aggregate, insert.

    Uses LIMIT/WHERE id > last batches rather than a server-side named cursor —
    Supabase's transaction pooler doesn't reliably keep a backend bound across
    cursor fetches, which manifests as the FETCH hanging indefinitely.
    """
    print("[rollup] paging GDELT article rows + ticker entities…", file=sys.stderr)

    started = time.monotonic()
    accs: dict[tuple[datetime, str], _BucketAcc] = defaultdict(_BucketAcc)
    fetched_per_window: dict[datetime, datetime] = {}
    n_seen = 0
    n_skipped_no_window = 0
    last_id = 0

    sql = """
        select
            a.id,
            a.source_fetched_at,
            a.raw_payload->>'_gdelt_file'           as gdelt_file,
            a.raw_payload->>'V1THEMES'              as themes_text,
            a.raw_payload->>'V15TONE'               as tone_text,
            a.raw_payload->>'V2DOCUMENTIDENTIFIER'  as url,
            a.raw_payload->>'V2SOURCECOMMONNAME'    as domain,
            coalesce(
                (select array_agg(e.entity_value)
                   from news_archive.article_entities e
                  where e.article_id = a.id and e.entity_type = 'ticker'),
                array[]::text[]
            ) as tickers
        from news_archive.articles a
        where a.source_id = %s
          and a.id > %s
        order by a.id
        limit %s
    """

    while True:
        with connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (source_id, last_id, batch_size))
            rows = cur.fetchall()
            conn.commit()
        if not rows:
            break

        for row in rows:
            n_seen += 1
            window_start = _window_start_from_file_url(row["gdelt_file"])
            if window_start is None:
                n_skipped_no_window += 1
                continue

            themes = _themes_from_cell(row["themes_text"])
            tickers = list(row["tickers"] or [])
            tone = parse_overall_tone(row["tone_text"])
            url = row["url"] or None
            domain = row["domain"] or None

            fa = row["source_fetched_at"]
            cur_fa = fetched_per_window.get(window_start)
            if cur_fa is None or fa > cur_fa:
                fetched_per_window[window_start] = fa

            for bucket in buckets_for_row(themes, tickers):
                accs[(window_start, bucket)].add(
                    domain=domain, url=url, tone=tone
                )

        last_id = rows[-1]["id"]
        elapsed = time.monotonic() - started
        print(
            f"[rollup]   processed {n_seen:,} rows  "
            f"({n_seen / max(elapsed, 0.001):.0f}/s)  "
            f"buckets so far: {len(accs):,}  last_id={last_id}",
            file=sys.stderr,
            flush=True,
        )

    elapsed = time.monotonic() - started
    print(
        f"[rollup] done streaming. rows={n_seen:,} "
        f"skipped(no window)={n_skipped_no_window} "
        f"unique buckets={len(accs):,} elapsed={elapsed:.1f}s",
        file=sys.stderr,
    )

    if not accs:
        print("[rollup] nothing to insert — done.", file=sys.stderr)
        return

    rollups: list[GdeltRollup] = [
        acc.to_rollup(
            window_start=window_start,
            fetched_at=fetched_per_window[window_start],
            theme_bucket=bucket,
        )
        for (window_start, bucket), acc in sorted(accs.items())
    ]

    print(f"[rollup] inserting {len(rollups):,} rollup rows…", file=sys.stderr)
    inserted, duplicate = insert_gdelt_rollups(rollups)
    print(
        f"[rollup] inserted={inserted:,} duplicate(skipped)={duplicate:,}",
        file=sys.stderr,
    )


def stage_delete(source_id: int, batch_size: int = 5000) -> None:
    """DELETE GDELT article rows in batches (cascades to article_entities)."""
    print(
        f"[delete] removing GDELT article rows in batches of {batch_size}…",
        file=sys.stderr,
    )
    total_deleted = 0
    started = time.monotonic()
    while True:
        with connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                with target as (
                    select id from news_archive.articles
                    where source_id = %s
                    order by id
                    limit %s
                )
                delete from news_archive.articles
                where id in (select id from target)
                """,
                (source_id, batch_size),
            )
            n = cur.rowcount
            conn.commit()
        total_deleted += n
        elapsed = time.monotonic() - started
        print(
            f"[delete]   batch -{n}  total={total_deleted:,}  "
            f"elapsed={elapsed:.1f}s  rate={total_deleted/elapsed:.0f}/s",
            file=sys.stderr,
        )
        if n == 0:
            break
    print(
        f"[delete] done. total deleted={total_deleted:,} elapsed={elapsed:.1f}s",
        file=sys.stderr,
    )


def stage_vacuum() -> None:
    """VACUUM FULL the two affected tables. Uses a direct autocommit connection
    because VACUUM cannot run in a transaction (and the pooler permits it
    when autocommit=True; verified in the previous shrink run).
    """
    print(
        "[vacuum] running VACUUM FULL on articles + article_entities…",
        file=sys.stderr,
    )
    for tbl in (
        "news_archive.articles",
        "news_archive.article_entities",
    ):
        started = time.monotonic()
        with psycopg.connect(
            settings.supabase_db_url.get_secret_value(),
            autocommit=True,
        ) as conn:
            conn.prepare_threshold = None
            with conn.cursor() as cur:
                cur.execute(f"vacuum full {tbl};")
        print(
            f"[vacuum]   {tbl}: done in {time.monotonic()-started:.1f}s",
            file=sys.stderr,
        )


def show_sizes(label: str) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select n.nspname || '.' || c.relname as t,
                   pg_size_pretty(pg_total_relation_size(c.oid)) as total
            from pg_class c
            join pg_namespace n on n.oid = c.relnamespace
            where c.relkind = 'r'
              and n.nspname = 'news_archive'
            order by pg_total_relation_size(c.oid) desc;
            """
        )
        print(f"\n=== sizes {label} ===", file=sys.stderr)
        for r in cur.fetchall():
            print(f"  {r['t']:42s}  {r['total']}", file=sys.stderr)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--rollup", action="store_true")
    p.add_argument("--delete", action="store_true")
    p.add_argument("--vacuum", action="store_true")
    p.add_argument("--all", action="store_true")
    p.add_argument("--batch", type=int, default=5000)
    args = p.parse_args()

    if not (args.rollup or args.delete or args.vacuum or args.all):
        p.print_help()
        return 2

    source_id = get_source_id_by_slug("gdelt_gkg")
    print(f"gdelt_gkg source_id = {source_id}", file=sys.stderr)
    show_sizes("BEFORE")

    if args.all or args.rollup:
        stage_rollup(source_id)

    if args.all or args.delete:
        stage_delete(source_id, batch_size=args.batch)

    if args.all or args.vacuum:
        stage_vacuum()

    show_sizes("AFTER")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

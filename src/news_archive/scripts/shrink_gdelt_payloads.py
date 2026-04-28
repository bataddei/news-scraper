"""One-shot backfill: rewrite existing GDELT rows with slim raw_payload + body.

Why: the original collector dumped the full ~14 KB GKG row into raw_payload.
The collector now writes a ~1 KB slim subset (see _RAW_PAYLOAD_KEEP_KEYS in
gdelt_gkg.py). This script applies the same shape to the ~120K historical
rows so we can VACUUM FULL and recover ~1 GB of TOAST space.

Dedup safety: GDELT articles dedup on (source_id, external_id=GKGRECORDID).
Body change does not affect that index. We keep content_hash unchanged on
existing rows — recomputing in pure SQL is impossible (Python NFKC normalize
is not in Postgres) and the (source_id, content_hash) index is still unique
because old hashes were already unique among themselves.

Recoverability: every dropped GKG field is recoverable per-row by re-fetching
raw_payload->>'_gdelt_file' (the URL of the 15-min GKG zip).

Run: `python -m news_archive.scripts.shrink_gdelt_payloads`
After it finishes, run VACUUM FULL outside this script (it cannot run through
a transaction pooler):

    psql "$SUPABASE_DB_URL_DIRECT" -c "vacuum full news_archive.articles;"

or paste into the Supabase SQL editor.
"""

from __future__ import annotations

import argparse
import sys
import time

from news_archive.db import connection, get_source_id_by_slug

# Must match _RAW_PAYLOAD_KEEP_KEYS in gdelt_gkg.py.
KEEP_KEYS: tuple[str, ...] = (
    "_gdelt_file",
    "_filter_reason",
    "GKGRECORDID",
    "V21DATE",
    "V2SOURCECOMMONNAME",
    "V2DOCUMENTIDENTIFIER",
    "V1THEMES",
    "V15TONE",
)

DEFAULT_BATCH = 5000


def count_to_update(source_id: int) -> int:
    """Rows still carrying fat raw_payload — proxy: any non-whitelisted key present."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select count(*)::bigint as n
            from news_archive.articles
            where source_id = %s
              and raw_payload is not null
              and exists (
                  select 1
                  from jsonb_object_keys(raw_payload) k
                  where k <> all(%s)
              )
            """,
            (source_id, list(KEEP_KEYS)),
        )
        row = cur.fetchone()
        assert row is not None
        return int(row["n"])


def update_batch(source_id: int, batch_size: int) -> int:
    """Update one batch. Returns the number of rows updated.

    We pick the smallest IDs that still have fat payloads so progress is
    monotonic and resumable after interruption.
    """
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with target as (
                select id
                from news_archive.articles
                where source_id = %(source_id)s
                  and raw_payload is not null
                  and exists (
                      select 1
                      from jsonb_object_keys(raw_payload) k
                      where k <> all(%(keep)s)
                  )
                order by id
                limit %(limit)s
            )
            update news_archive.articles a
            set
                raw_payload = (
                    select coalesce(jsonb_object_agg(key, value), '{}'::jsonb)
                    from jsonb_each(a.raw_payload)
                    where key = any(%(keep)s)
                ),
                body =
                    'url=' || coalesce(a.raw_payload->>'V2DOCUMENTIDENTIFIER','') || E'\n' ||
                    'themes=' || coalesce(a.raw_payload->>'V1THEMES','') || E'\n' ||
                    'tone=' || coalesce(a.raw_payload->>'V15TONE','')
            from target
            where a.id = target.id
            """,
            {"source_id": source_id, "keep": list(KEEP_KEYS), "limit": batch_size},
        )
        n = cur.rowcount
        conn.commit()
        return n


def show_table_size() -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select pg_size_pretty(pg_total_relation_size('news_archive.articles'))
                as total,
                   pg_size_pretty(pg_relation_size('news_archive.articles'))
                as heap,
                   pg_size_pretty(
                       pg_total_relation_size('news_archive.articles')
                     - pg_relation_size('news_archive.articles')
                     - pg_indexes_size('news_archive.articles')
                   ) as toast
            """
        )
        row = cur.fetchone()
        assert row is not None
        print(
            f"  articles: total={row['total']}  heap={row['heap']}  toast={row['toast']}",
            file=sys.stderr,
        )


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    p.add_argument("--max-batches", type=int, default=0,
                   help="Stop after N batches (0 = run until done).")
    args = p.parse_args()

    source_id = get_source_id_by_slug("gdelt_gkg")
    print(f"gdelt_gkg source_id = {source_id}", file=sys.stderr)
    print("Sizes BEFORE:", file=sys.stderr)
    show_table_size()

    remaining = count_to_update(source_id)
    print(f"rows needing slim: {remaining:,}", file=sys.stderr)
    if remaining == 0:
        print("nothing to do.", file=sys.stderr)
        return 0

    started = time.monotonic()
    total = 0
    batches = 0
    while True:
        n = update_batch(source_id, args.batch)
        total += n
        batches += 1
        elapsed = time.monotonic() - started
        rate = total / elapsed if elapsed > 0 else 0.0
        print(
            f"batch {batches}: +{n} rows  total={total:,}/{remaining:,}  "
            f"elapsed={elapsed:.1f}s  rate={rate:.0f}/s",
            file=sys.stderr,
        )
        if n == 0:
            break
        if args.max_batches and batches >= args.max_batches:
            print("hit --max-batches, stopping early", file=sys.stderr)
            break

    print("Sizes AFTER updates (TOAST will only shrink after VACUUM FULL):",
          file=sys.stderr)
    show_table_size()
    print(
        "\nNext step — reclaim space (cannot run through transaction pooler):\n"
        "  vacuum full news_archive.articles;\n"
        "Run via Supabase SQL editor or `psql` against the direct connection.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

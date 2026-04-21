"""Postgres access helpers built on psycopg v3.

We use a small global connection pool rather than per-call connections because
the Supabase transaction pooler charges per connection-second; keeping a handful
of warm connections is cheaper and faster than constant reconnects.

All inserts go through typed helpers so callers can't forget a timestamp.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from news_archive.config import settings
from news_archive.models import Article, ArticleEntity, CollectionRun

_pool: ConnectionPool | None = None


def _configure_connection(conn: psycopg.Connection) -> None:
    """Disable server-side prepared statements.

    Supabase's Transaction pooler (Supavisor / pgbouncer in transaction mode)
    rotates backend connections between transactions, so a prepared statement
    registered on one backend is invisible — or a name collision — on the next.
    Setting `prepare_threshold=None` tells psycopg to never auto-prepare, which
    is the canonical fix when connecting through a transaction pooler.
    """
    conn.prepare_threshold = None


def get_pool() -> ConnectionPool:
    """Lazy-initialized process-wide pool. Small by default (droplet has 1 vCPU)."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=settings.supabase_db_url.get_secret_value(),
            min_size=1,
            max_size=4,
            kwargs={"row_factory": dict_row, "autocommit": False},
            configure=_configure_connection,
            open=True,
        )
    return _pool


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


def close_pool() -> None:
    """Close the pool at process shutdown. Safe to call multiple times."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------

def get_source_id_by_slug(slug: str) -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "select id from news_archive.sources where slug = %s",
            (slug,),
        )
        row = cur.fetchone()
        if row is None:
            raise LookupError(f"no source with slug={slug!r} — is the seed migration applied?")
        return int(row["id"])


# ---------------------------------------------------------------------------
# collection_runs
# ---------------------------------------------------------------------------

def start_collection_run(run: CollectionRun) -> int:
    """Insert a `running` row and return its id. Call at the top of every collector."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into news_archive.collection_runs
                (source_id, started_at, status, notes)
            values (%s, %s, 'running', %s)
            returning id
            """,
            (run.source_id, run.started_at, run.notes),
        )
        row = cur.fetchone()
        assert row is not None
        conn.commit()
        return int(row["id"])


def finish_collection_run(run: CollectionRun) -> None:
    """Update the row with final status and counters. Call once the collector exits."""
    if run.id is None:
        raise ValueError("finish_collection_run requires run.id (set by start_collection_run)")
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update news_archive.collection_runs
               set finished_at        = %s,
                   status             = %s,
                   articles_seen      = %s,
                   articles_inserted  = %s,
                   articles_duplicate = %s,
                   error_message      = %s,
                   notes              = coalesce(%s, notes)
             where id = %s
            """,
            (
                run.finished_at,
                run.status,
                run.articles_seen,
                run.articles_inserted,
                run.articles_duplicate,
                run.error_message,
                run.notes,
                run.id,
            ),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# articles
# ---------------------------------------------------------------------------

def insert_article(article: Article, entities: list[ArticleEntity] | None = None) -> int | None:
    """Insert an article. Returns the new id, or None if it was a dedup hit.

    Dedup is enforced by two partial-unique indexes (see 0002_indexes.sql). We
    use `ON CONFLICT DO NOTHING` so a duplicate is not an error — it's the
    expected outcome when a feed re-serves the same item.
    """
    raw_json = json.dumps(article.raw_payload) if article.raw_payload is not None else None

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into news_archive.articles
                (source_id, external_id, url, headline, body, author,
                 source_published_at, source_fetched_at,
                 raw_payload, content_hash, language)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
            on conflict do nothing
            returning id
            """,
            (
                article.source_id,
                article.external_id,
                article.url,
                article.headline,
                article.body,
                article.author,
                article.source_published_at,
                article.source_fetched_at,
                raw_json,
                article.content_hash,
                article.language,
            ),
        )
        row = cur.fetchone()
        if row is None:
            conn.commit()
            return None
        new_id = int(row["id"])

        if entities:
            cur.executemany(
                """
                insert into news_archive.article_entities
                    (article_id, entity_type, entity_value, confidence)
                values (%s, %s, %s, %s)
                """,
                [(new_id, e.entity_type, e.entity_value, e.confidence) for e in entities],
            )
        conn.commit()
        return new_id


def fetch_article_by_id(article_id: int) -> dict[str, Any] | None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "select * from news_archive.articles where id = %s",
            (article_id,),
        )
        return cur.fetchone()

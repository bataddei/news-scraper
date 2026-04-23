"""Postgres helpers for the `literature.*` schema.

Shares the connection pool with `news_archive.db` — same Supabase project,
same transaction pooler, same `prepare_threshold=None` config. Only the
per-table SQL is schema-specific.

All inserts go through typed helpers so callers can't forget a timestamp.
"""

from __future__ import annotations

import json

from news_archive.db import connection
from news_archive.literature.models import LitCollectionRun, Paper, TriageRecord

# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------

def get_source_id_by_slug(slug: str) -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "select id from literature.sources where slug = %s",
            (slug,),
        )
        row = cur.fetchone()
        if row is None:
            raise LookupError(
                f"no literature source with slug={slug!r} — is the seed migration applied?"
            )
        return int(row["id"])


# ---------------------------------------------------------------------------
# collection_runs
# ---------------------------------------------------------------------------

def start_collection_run(run: LitCollectionRun) -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into literature.collection_runs
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


def finish_collection_run(run: LitCollectionRun) -> None:
    if run.id is None:
        raise ValueError("finish_collection_run requires run.id (set by start_collection_run)")
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update literature.collection_runs
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
# papers
# ---------------------------------------------------------------------------

def insert_paper(paper: Paper) -> int | None:
    """Insert a paper. Returns the new id, or None if it was a dedup hit.

    Dedup is enforced by two partial-unique indexes (see 0006_literature_indexes.sql).
    `ON CONFLICT DO NOTHING` so duplicates are the expected outcome on re-poll.
    """
    raw_json = json.dumps(paper.raw_payload) if paper.raw_payload is not None else None

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into literature.papers
                (source_id, external_id, url, pdf_url, title, authors, abstract,
                 categories, keywords,
                 source_published_at, source_fetched_at,
                 raw_payload, content_hash)
            values (%s, %s, %s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s::jsonb, %s)
            on conflict do nothing
            returning id
            """,
            (
                paper.source_id,
                paper.external_id,
                paper.url,
                paper.pdf_url,
                paper.title,
                paper.authors,
                paper.abstract,
                paper.categories,
                paper.keywords,
                paper.source_published_at,
                paper.source_fetched_at,
                raw_json,
                paper.content_hash,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"]) if row is not None else None


# ---------------------------------------------------------------------------
# triage
# ---------------------------------------------------------------------------

def insert_triage(triage: TriageRecord) -> int | None:
    """Insert a triage row. Returns the new id, or None if (paper_id, triage_version)
    already exists — re-running the triage job for the same version is a no-op.
    """
    raw_json = json.dumps(triage.raw_response) if triage.raw_response is not None else None

    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into literature.triage
                (paper_id, triage_version, model_used,
                 score_systematic_futures, score_short_timeframe, score_empirical_rigor,
                 score_data_accessibility, score_implementation_effort, overall_priority,
                 claimed_edge, required_data, method_summary, red_flags, reasoning,
                 raw_response)
            values (%s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s::jsonb)
            on conflict do nothing
            returning id
            """,
            (
                triage.paper_id,
                triage.triage_version,
                triage.model_used,
                triage.score_systematic_futures,
                triage.score_short_timeframe,
                triage.score_empirical_rigor,
                triage.score_data_accessibility,
                triage.score_implementation_effort,
                triage.overall_priority,
                triage.claimed_edge,
                triage.required_data,
                triage.method_summary,
                triage.red_flags,
                triage.reasoning,
                raw_json,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row["id"]) if row is not None else None

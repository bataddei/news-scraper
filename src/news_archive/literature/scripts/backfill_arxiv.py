"""One-shot arXiv q-fin backfill.

Usage:
    python -m news_archive.literature.scripts.backfill_arxiv [--since YYYY-MM-DD] [--dry-run]

Walks the arXiv API in descending submittedDate order, paginated, and inserts
every matching paper into `literature.papers`. Safe to re-run — dedup on
(source_id, external_id) collapses duplicates to no-ops.

Logged as a single `literature.collection_runs` row with
`notes='backfill since=YYYY-MM-DD'` so the daily integrity report can see it
without confusing gap detection (the daily run_id continues to advance).

Politeness:
    * 3-second sleep between API pages (arXiv's published convention).
    * User-Agent carries operator contact email via news_archive.http.
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import UTC, datetime

from news_archive import http
from news_archive.collectors.base import utcnow
from news_archive.db import close_pool
from news_archive.literature import db as lit_db
from news_archive.literature.collectors.arxiv_qfin import (
    ARXIV_API_URL,
    DEFAULT_CATEGORIES,
    build_search_query,
    entry_to_paper,
    parse_feed,
)
from news_archive.literature.models import LitCollectionRun
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

DEFAULT_SINCE = "2025-01-01"   # operator-specified backfill floor
PAGE_SIZE = 200                # arXiv tolerates up to ~2000; 200 keeps responses snappy
POLITENESS_SECONDS = 3.0       # arXiv's documented minimum between requests
SOURCE_SLUG = "arxiv_qfin"


def _parse_since(value: str) -> datetime:
    dt = datetime.strptime(value, "%Y-%m-%d")
    return dt.replace(tzinfo=UTC)


def _build_params(since: datetime, *, start: int) -> dict[str, object]:
    return {
        "search_query": build_search_query(DEFAULT_CATEGORIES, submitted_since=since),
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "start": start,
        "max_results": PAGE_SIZE,
    }


def _iter_pages(since: datetime) -> object:
    """Yield (fetched_at, feed) tuples one page at a time until the API returns empty."""
    start = 0
    while True:
        params = _build_params(since, start=start)
        log.info("backfill.page_fetch", start=start, page_size=PAGE_SIZE)
        raw = http.fetch_bytes(ARXIV_API_URL, params=params)
        fetched_at = utcnow()

        feed = parse_feed(raw)
        if feed.bozo:
            log.warning(
                "backfill.page_parse_warning",
                start=start,
                bozo_reason=str(feed.bozo_exception),
            )
        n_entries = len(feed.entries)
        log.info("backfill.page_loaded", start=start, entries=n_entries)

        if n_entries == 0:
            return

        yield fetched_at, feed

        start += n_entries
        time.sleep(POLITENESS_SECONDS)


def _run_backfill(since: datetime, *, dry_run: bool) -> tuple[int, int, int, int]:
    """Return (seen, inserted, duplicate, failed)."""
    source_id = lit_db.get_source_id_by_slug(SOURCE_SLUG)

    # Register the run up front so its progress is visible even mid-backfill.
    run: LitCollectionRun | None = None
    if not dry_run:
        run = LitCollectionRun(
            source_id=source_id,
            started_at=utcnow(),
            notes=f"backfill since={since.date().isoformat()}",
        )
        run.id = lit_db.start_collection_run(run)
        log.info("backfill.run_started", run_id=run.id, since=since.date().isoformat())

    seen = inserted = duplicate = failed = 0
    status = "success"
    error_message: str | None = None

    try:
        for fetched_at, feed in _iter_pages(since):
            for entry in feed.entries:
                seen += 1
                paper = entry_to_paper(
                    entry,
                    source_id=source_id,
                    fetched_at=fetched_at,
                    logger=log,
                )
                if paper is None:
                    failed += 1
                    continue
                if dry_run:
                    inserted += 1
                    continue
                try:
                    new_id = lit_db.insert_paper(paper)
                    if new_id is None:
                        duplicate += 1
                    else:
                        inserted += 1
                except Exception as e:
                    failed += 1
                    log.warning(
                        "backfill.insert_failed",
                        error=str(e),
                        external_id=paper.external_id,
                        title=paper.title[:120],
                    )
        if failed > 0:
            status = "partial"
            error_message = f"{failed} per-paper failure(s); see warnings"
    except Exception as e:
        status = "failed"
        error_message = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        log.error("backfill.aborted", error=str(e))
    finally:
        if run is not None and not dry_run:
            run.finished_at = utcnow()
            run.status = status
            run.articles_seen = seen
            run.articles_inserted = inserted
            run.articles_duplicate = duplicate
            run.error_message = error_message
            lit_db.finish_collection_run(run)

    return seen, inserted, duplicate, failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill arXiv q-fin papers into literature.papers.")
    parser.add_argument(
        "--since",
        default=DEFAULT_SINCE,
        help="Backfill floor date, YYYY-MM-DD (default: 2025-01-01).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk the API and log counts, but don't insert anything.",
    )
    args = parser.parse_args(argv)

    configure_logging()
    try:
        since = _parse_since(args.since)
    except ValueError:
        log.error("backfill.bad_since", value=args.since)
        return 2

    log.info("backfill.start", since=since.date().isoformat(), dry_run=args.dry_run)

    try:
        seen, inserted, duplicate, failed = _run_backfill(since, dry_run=args.dry_run)
    finally:
        close_pool()

    log.info(
        "backfill.done",
        seen=seen,
        inserted=inserted,
        duplicate=duplicate,
        failed=failed,
        dry_run=args.dry_run,
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

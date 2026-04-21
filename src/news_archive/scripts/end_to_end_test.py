"""End-to-end foundation test.

Writes a fake article, reads it back, asserts:
  * all three timestamps are populated,
  * source_fetched_at and source_published_at are both UTC and within the
    window we set,
  * db_inserted_at is set by the database default (not the client).

Also inserts a second copy to confirm `content_hash` dedup works. Finally
deletes the fake rows so the archive stays clean.

Run: `make e2e` or `python -m news_archive.scripts.end_to_end_test`.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta

from news_archive import db
from news_archive.collectors.base import utcnow
from news_archive.hashing import content_hash
from news_archive.logging_config import configure_logging, get_logger
from news_archive.models import Article, ArticleEntity, CollectionRun

log = get_logger(__name__)

FAKE_SOURCE_SLUG = "fed_fomc_statements"  # any seeded slug works
FAKE_HEADLINE = "[E2E-TEST] synthetic row — safe to delete"


def _cleanup(source_id: int) -> None:
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from news_archive.articles where source_id = %s and headline = %s",
            (source_id, FAKE_HEADLINE),
        )
        cur.execute(
            "delete from news_archive.collection_runs where source_id = %s and notes = 'e2e-test'",
            (source_id,),
        )
        conn.commit()


def main() -> int:
    configure_logging()
    log.info("e2e.start")

    source_id = db.get_source_id_by_slug(FAKE_SOURCE_SLUG)
    _cleanup(source_id)

    run = CollectionRun(source_id=source_id, started_at=utcnow(), notes="e2e-test")
    run.id = db.start_collection_run(run)

    published = datetime(2024, 6, 1, 12, 30, tzinfo=UTC)
    fetched = utcnow()
    body = "This is a synthetic article used to verify the three-timestamp protocol."

    article = Article(
        source_id=source_id,
        external_id="E2E-TEST-0001",
        url="https://example.invalid/e2e-test",
        headline=FAKE_HEADLINE,
        body=body,
        source_published_at=published,
        source_fetched_at=fetched,
        raw_payload={"note": "e2e test payload"},
        content_hash=content_hash(FAKE_HEADLINE, body),
        language="en",
    )
    entities = [ArticleEntity(entity_type="event", entity_value="FOMC")]

    # --- first insert: should succeed ---
    new_id = db.insert_article(article, entities)
    if new_id is None:
        log.error("e2e.first_insert_dedup_hit_unexpectedly")
        return 1
    log.info("e2e.first_insert_ok", article_id=new_id)

    # --- second insert: should dedup ---
    dup_id = db.insert_article(article)
    if dup_id is not None:
        log.error("e2e.dedup_failed", duplicate_id=dup_id)
        return 1
    log.info("e2e.dedup_ok")

    # --- read back and verify ---
    row = db.fetch_article_by_id(new_id)
    if row is None:
        log.error("e2e.readback_missing")
        return 1

    errors: list[str] = []

    for col in ("source_published_at", "source_fetched_at", "db_inserted_at"):
        v = row.get(col)
        if v is None:
            errors.append(f"{col} is NULL")
        elif v.tzinfo is None:
            errors.append(f"{col} is timezone-naive")

    if row["source_published_at"] != published:
        errors.append(f"source_published_at roundtrip mismatch: {row['source_published_at']} != {published}")

    fetched_diff = abs((row["source_fetched_at"] - fetched).total_seconds())
    if fetched_diff > 2:
        errors.append(f"source_fetched_at drifted by {fetched_diff}s")

    now_ref = utcnow()
    inserted_diff = (now_ref - row["db_inserted_at"]).total_seconds()
    if not (timedelta(seconds=-5).total_seconds() <= inserted_diff <= 60):
        errors.append(f"db_inserted_at out of expected window: diff={inserted_diff}s")

    if errors:
        for e in errors:
            log.error("e2e.check_failed", detail=e)
        run.status = "failed"
        run.error_message = "; ".join(errors)
        run.finished_at = utcnow()
        db.finish_collection_run(run)
        return 1

    run.articles_seen = 2
    run.articles_inserted = 1
    run.articles_duplicate = 1
    run.status = "success"
    run.finished_at = utcnow()
    db.finish_collection_run(run)

    log.info(
        "e2e.pass",
        article_id=new_id,
        source_published_at=row["source_published_at"].isoformat(),
        source_fetched_at=row["source_fetched_at"].isoformat(),
        db_inserted_at=row["db_inserted_at"].isoformat(),
    )

    _cleanup(source_id)
    log.info("e2e.cleanup_done")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Base class every collector extends.

Enforces the project's non-negotiables:
  * A `collection_runs` row is opened before work starts and closed after,
    with `finished_at` and `status` always populated.
  * An exception inside the collector is logged and recorded on the run row,
    not re-raised — one source must never crash the pipeline.
  * `source_fetched_at` is captured by the collector (the `fetch_*` helpers
    return a timezone-aware UTC datetime).
  * The three timestamps are required on every `Article` the collector yields;
    the pydantic model enforces this at construction.
"""

from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import UTC, datetime

from news_archive import db
from news_archive.logging_config import get_logger
from news_archive.models import Article, ArticleEntity, CollectionRun

log = get_logger(__name__)


def utcnow() -> datetime:
    """Timezone-aware UTC now. Use everywhere a timestamp is set."""
    return datetime.now(UTC)


class BaseCollector(ABC):
    """Subclass this and implement `collect()`."""

    #: slug in `news_archive.sources`. Must match a seeded row.
    source_slug: str

    def __init__(self) -> None:
        if not getattr(self, "source_slug", None):
            raise ValueError(f"{type(self).__name__}.source_slug must be set")
        self.source_id: int = db.get_source_id_by_slug(self.source_slug)
        self.logger = log.bind(source=self.source_slug, source_id=self.source_id)

    @abstractmethod
    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        """Yield `(article, entities)` tuples. Entities list may be empty."""

    def run(self, notes: str | None = None) -> CollectionRun:
        """Run one collection pass. Idempotent — safe to run repeatedly."""
        run = CollectionRun(
            source_id=self.source_id,
            started_at=utcnow(),
            notes=notes,
        )
        run.id = db.start_collection_run(run)
        run_log = self.logger.bind(run_id=run.id)
        run_log.info("collection_run.start")

        try:
            for article, entities in self.collect():
                run.articles_seen += 1
                try:
                    new_id = db.insert_article(article, entities)
                    if new_id is None:
                        run.articles_duplicate += 1
                    else:
                        run.articles_inserted += 1
                except Exception as e:
                    run_log.warning(
                        "article.insert_failed",
                        error=str(e),
                        url=article.url,
                        headline=article.headline[:120],
                    )
            run.status = "success"
        except Exception as e:
            run.status = "failed"
            run.error_message = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            run_log.error("collection_run.failed", error=str(e))
        finally:
            run.finished_at = utcnow()
            db.finish_collection_run(run)
            run_log.info(
                "collection_run.finish",
                status=run.status,
                seen=run.articles_seen,
                inserted=run.articles_inserted,
                duplicate=run.articles_duplicate,
            )
        return run

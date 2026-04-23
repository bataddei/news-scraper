"""Base class every literature collector extends.

Parallel to `news_archive.collectors.base.BaseCollector`. Enforces the same
non-negotiables (collection_runs row opened/closed, exceptions logged not
raised, tz-aware timestamps) but reads/writes the `literature.*` schema.
"""

from __future__ import annotations

import traceback
from abc import ABC, abstractmethod
from collections.abc import Iterable

from news_archive.collectors.base import utcnow
from news_archive.literature import db as lit_db
from news_archive.literature.models import LitCollectionRun, Paper
from news_archive.logging_config import get_logger

log = get_logger(__name__)


class LitBaseCollector(ABC):
    """Subclass this and implement `collect()`."""

    #: slug in `literature.sources`. Must match a seeded row.
    source_slug: str

    def __init__(self) -> None:
        if not getattr(self, "source_slug", None):
            raise ValueError(f"{type(self).__name__}.source_slug must be set")
        self.source_id: int = lit_db.get_source_id_by_slug(self.source_slug)
        self.logger = log.bind(source=self.source_slug, source_id=self.source_id)

    @abstractmethod
    def collect(self) -> Iterable[Paper]:
        """Yield `Paper` instances. Unlike news_archive, no entity tagging here —
        paper metadata (authors, categories, keywords) lives on the row itself."""

    def run(self, notes: str | None = None) -> LitCollectionRun:
        """Run one collection pass. Idempotent — safe to run repeatedly."""
        run = LitCollectionRun(
            source_id=self.source_id,
            started_at=utcnow(),
            notes=notes,
        )
        run.id = lit_db.start_collection_run(run)
        run_log = self.logger.bind(run_id=run.id)
        run_log.info("collection_run.start")

        failed = 0
        try:
            for paper in self.collect():
                run.articles_seen += 1
                try:
                    new_id = lit_db.insert_paper(paper)
                    if new_id is None:
                        run.articles_duplicate += 1
                    else:
                        run.articles_inserted += 1
                except Exception as e:
                    failed += 1
                    run_log.warning(
                        "paper.insert_failed",
                        error=str(e),
                        url=paper.url,
                        title=paper.title[:120],
                    )
            run.status = "partial" if failed > 0 else "success"
            if failed > 0:
                run.error_message = f"{failed} per-paper insert(s) failed; see warnings"
        except Exception as e:
            run.status = "failed"
            run.error_message = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            run_log.error("collection_run.failed", error=str(e))
        finally:
            run.finished_at = utcnow()
            lit_db.finish_collection_run(run)
            run_log.info(
                "collection_run.finish",
                status=run.status,
                seen=run.articles_seen,
                inserted=run.articles_inserted,
                duplicate=run.articles_duplicate,
                failed=failed,
            )
        return run

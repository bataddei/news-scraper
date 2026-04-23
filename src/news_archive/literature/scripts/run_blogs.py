"""Run every active blog_* source in one invocation.

Usage:
    python -m news_archive.literature.scripts.run_blogs

Queries literature.sources for slugs matching `blog_%` where active=true,
and runs each through BlogFeedCollector. Each blog gets its own
literature.collection_runs row, so gap detection can flag a single dead
feed without silencing the others.

A failure in one blog is caught and logged; the driver continues to the
next. Exit code is 0 iff every feed completed with status=success.

Cron cadence (daily-ish is fine; blogs post infrequently):
    0 */6 * * *   news ... python -m news_archive.literature.scripts.run_blogs
"""

from __future__ import annotations

import sys

from news_archive.db import close_pool
from news_archive.literature import db as lit_db
from news_archive.literature.collectors.blog_rss import BlogFeedCollector
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

BLOG_SLUG_PREFIX = "blog_"


def main(argv: list[str] | None = None) -> int:
    configure_logging()

    try:
        slugs = lit_db.list_active_slugs_by_prefix(BLOG_SLUG_PREFIX)
    except Exception as exc:
        log.error("run_blogs.discovery_failed", error=str(exc))
        close_pool()
        return 1

    if not slugs:
        log.warning("run_blogs.no_active_blogs")
        close_pool()
        return 0

    log.info("run_blogs.start", slugs=slugs)

    any_failed = False
    try:
        for slug in slugs:
            try:
                run = BlogFeedCollector(slug).run()
            except Exception as exc:
                # Base-class run() catches its own exceptions, but __init__ can
                # fail (e.g. feed_url NULL). Don't let one dead row kill the batch.
                any_failed = True
                log.error("run_blogs.collector_crashed", slug=slug, error=str(exc))
                continue
            if run.status != "success":
                any_failed = True
            log.info(
                "run_blogs.one_done",
                slug=slug,
                status=run.status,
                inserted=run.articles_inserted,
                duplicate=run.articles_duplicate,
                seen=run.articles_seen,
            )
    finally:
        close_pool()

    return 1 if any_failed else 0


if __name__ == "__main__":
    sys.exit(main())

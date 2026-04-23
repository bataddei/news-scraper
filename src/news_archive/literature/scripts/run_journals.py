"""Run every active journal_* source in one invocation.

Usage:
    python -m news_archive.literature.scripts.run_journals

Sibling of scripts/run_blogs.py. Same driver pattern, different slug prefix.
Journal RSS feeds are lower volume than blogs (weekly-to-monthly articles)
and typically expose abstracts + metadata rather than full text — that is
enough for the triage pass downstream.

Cron cadence:
    0 7 * * *   news ... python -m news_archive.literature.scripts.run_journals

Journals marked inactive (e.g. RFS, JPM behind Cloudflare) are silently
skipped by the discovery query — no code path change required.
"""

from __future__ import annotations

import sys

from news_archive.db import close_pool
from news_archive.literature import db as lit_db
from news_archive.literature.collectors.rss_feed import RssFeedCollector
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

JOURNAL_SLUG_PREFIX = "journal_"


def main(argv: list[str] | None = None) -> int:
    configure_logging()

    try:
        slugs = lit_db.list_active_slugs_by_prefix(JOURNAL_SLUG_PREFIX)
    except Exception as exc:
        log.error("run_journals.discovery_failed", error=str(exc))
        close_pool()
        return 1

    if not slugs:
        log.warning("run_journals.no_active_journals")
        close_pool()
        return 0

    log.info("run_journals.start", slugs=slugs)

    any_failed = False
    try:
        for slug in slugs:
            try:
                run = RssFeedCollector(slug).run()
            except Exception as exc:
                any_failed = True
                log.error("run_journals.collector_crashed", slug=slug, error=str(exc))
                continue
            if run.status != "success":
                any_failed = True
            log.info(
                "run_journals.one_done",
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

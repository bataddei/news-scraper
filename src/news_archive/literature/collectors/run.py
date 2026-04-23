"""CLI entry point for literature collectors.

Usage:
    python -m news_archive.literature.collectors.run <slug>

Parallel to `news_archive.collectors.run` but isolated: each dispatcher only
imports its own world, so a syntax error in a news collector can't take down
literature collection and vice versa.
"""

from __future__ import annotations

import sys

from news_archive.db import close_pool
from news_archive.literature.collectors.arxiv_qfin import ArxivQfinCollector
from news_archive.literature.collectors.base import LitBaseCollector
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

COLLECTORS: dict[str, type[LitBaseCollector]] = {
    "arxiv_qfin": ArxivQfinCollector,
    # blog_*, journal_*, ssrn_fen — added as each collector ships.
}


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = argv if argv is not None else sys.argv[1:]

    if len(args) != 1:
        log.error(
            "run.usage",
            message="usage: python -m news_archive.literature.collectors.run <slug>",
        )
        return 2

    slug = args[0]
    cls = COLLECTORS.get(slug)
    if cls is None:
        log.error(
            "run.unknown_slug",
            slug=slug,
            known=sorted(COLLECTORS.keys()),
        )
        return 2

    try:
        collector = cls()
        run = collector.run()
    finally:
        close_pool()

    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    sys.exit(main())

"""CLI for gap detection — `python -m news_archive.scripts.gap_check`.

Prints one line per gap to stdout and logs structured events. Exits:
  * 0 if no gaps,
  * 1 if one or more sources are overdue or never-run.

A non-zero exit is useful for cron integration: if the daily report
script calls this first, a gap run can fail-loud via healthchecks.io.
"""

from __future__ import annotations

import sys

from news_archive import db
from news_archive.logging_config import configure_logging, get_logger
from news_archive.monitoring.gaps import check_gaps_now, format_gap

log = get_logger(__name__)


def main() -> int:
    configure_logging()
    try:
        with db.connection() as conn:
            gaps = check_gaps_now(conn)
    finally:
        db.close_pool()

    if not gaps:
        log.info("gap_check.ok")
        print("No gaps detected.")
        return 0

    log.warning("gap_check.found", count=len(gaps))
    for gap in gaps:
        line = format_gap(gap)
        print(line)
        log.warning(
            "gap_check.gap",
            source=gap.source_slug,
            kind=gap.kind,
            seconds_since_last=gap.seconds_since_last,
            max_gap_seconds=gap.max_gap_seconds,
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())

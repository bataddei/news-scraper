"""CLI — `python -m news_archive.scripts.daily_report`.

Fetches data, formats the message, posts to Telegram. Designed to be
invoked from cron at 08:00 UTC daily (see `deploy/cron/news-pipeline.cron`).

Exit codes:
  * 0 on successful send,
  * 1 if any step fails (DB unreachable, Telegram API error, etc.)

The report runs even if there are gaps — the gaps are *part of* the
report. Alerting on gaps is a separate concern handled by gap_check.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from news_archive import db
from news_archive.collectors.run import COLLECTORS
from news_archive.config import settings
from news_archive.logging_config import configure_logging, get_logger
from news_archive.monitoring.gaps import check_gaps_now
from news_archive.monitoring.report import (
    fetch_report_data,
    format_report,
    send_telegram,
)

log = get_logger(__name__)


def main() -> int:
    configure_logging()

    bot_token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id
    if bot_token is None or not chat_id:
        log.error(
            "daily_report.missing_telegram_config",
            has_token=bot_token is not None,
            has_chat_id=bool(chat_id),
        )
        return 1

    try:
        with db.connection() as conn:
            data = fetch_report_data(
                conn,
                generated_at=datetime.now(UTC),
                only_slugs=sorted(COLLECTORS.keys()),
            )
            gaps = check_gaps_now(conn)
    except Exception as exc:
        log.error("daily_report.fetch_failed", error=str(exc))
        db.close_pool()
        return 1

    data = type(data)(
        generated_at=data.generated_at,
        per_source=data.per_source,
        total_articles=data.total_articles,
        db_size_pretty=data.db_size_pretty,
        disk=data.disk,
        gaps=gaps,
    )

    text = format_report(data)
    log.info("daily_report.sending", length=len(text), gap_count=len(gaps))

    try:
        send_telegram(
            text,
            bot_token=bot_token.get_secret_value(),
            chat_id=str(chat_id),
        )
    except Exception as exc:
        log.error("daily_report.send_failed", error=str(exc))
        db.close_pool()
        return 1
    finally:
        db.close_pool()

    log.info("daily_report.ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())

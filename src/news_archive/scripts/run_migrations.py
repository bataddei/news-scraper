"""Apply every .sql file in /migrations in version order.

Each migration's filename is recorded in `news_archive.schema_migrations`; we skip
filenames already recorded. Safe to re-run. Runs each migration in its own
transaction so a failure doesn't leave the DB half-migrated.
"""

from __future__ import annotations

import sys
from pathlib import Path

from news_archive.config import settings
from news_archive.db import connection, close_pool
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"

TRACKING_TABLE_SQL = """
create schema if not exists news_archive;
create table if not exists news_archive.schema_migrations (
    filename    text primary key,
    applied_at  timestamptz not null default (now() at time zone 'utc')
);
"""


def _applied_filenames() -> set[str]:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(TRACKING_TABLE_SQL)
        conn.commit()
        cur.execute("select filename from news_archive.schema_migrations")
        return {row["filename"] for row in cur.fetchall()}


def _apply_one(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    with connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cur.execute(
            "insert into news_archive.schema_migrations (filename) values (%s)",
            (path.name,),
        )
        conn.commit()


def main() -> int:
    configure_logging()
    log.info("migrations.start", db_env=settings.environment, dir=str(MIGRATIONS_DIR))

    if not MIGRATIONS_DIR.is_dir():
        log.error("migrations.dir_missing", path=str(MIGRATIONS_DIR))
        return 1

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not files:
        log.warning("migrations.none_found")
        return 0

    applied = _applied_filenames()
    pending = [f for f in files if f.name not in applied]

    log.info("migrations.plan", total=len(files), already_applied=len(applied), pending=len(pending))

    if not pending:
        log.info("migrations.up_to_date")
        close_pool()
        return 0

    for path in pending:
        log.info("migrations.apply", filename=path.name)
        try:
            _apply_one(path)
        except Exception as e:
            log.error("migrations.failed", filename=path.name, error=str(e))
            close_pool()
            return 1

    log.info("migrations.done", applied=len(pending))
    close_pool()
    return 0


if __name__ == "__main__":
    sys.exit(main())

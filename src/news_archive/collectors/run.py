"""CLI entry point for collectors — `python -m news_archive.collectors.run <slug>`.

Used by the systemd template (`news-collector@<slug>.service`) and by cron.
The mapping slug → class is explicit so a typo fails loudly rather than
silently importing the wrong module.
"""

from __future__ import annotations

import sys

from news_archive.collectors.base import BaseCollector
from news_archive.collectors.bls_releases import BLSReleasesCollector
from news_archive.collectors.econ_calendar_ff import ForexFactoryCalendarCollector
from news_archive.collectors.fed_fomc_statements import FOMCStatementsCollector
from news_archive.collectors.fed_speeches import FedSpeechesCollector
from news_archive.collectors.sec_edgar_mag7 import SECEdgarMag7Collector
from news_archive.collectors.treasury_press import TreasuryPressCollector
from news_archive.collectors.wires import (
    GlobeNewswireCollector,
    PRNewswireCollector,
)
from news_archive.db import close_pool
from news_archive.logging_config import configure_logging, get_logger

log = get_logger(__name__)

COLLECTORS: dict[str, type[BaseCollector]] = {
    "fed_fomc_statements": FOMCStatementsCollector,
    "fed_speeches": FedSpeechesCollector,
    "bls_releases": BLSReleasesCollector,
    "treasury_press": TreasuryPressCollector,
    "sec_edgar_mag7": SECEdgarMag7Collector,
    "wire_pr_newswire": PRNewswireCollector,
    "wire_globenewswire": GlobeNewswireCollector,
    "econ_calendar_ff": ForexFactoryCalendarCollector,
    # wire_business_wire: defined but not registered — Business Wire returns 403
    # to public crawlers. Re-enable once we have viable access.
}


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = argv if argv is not None else sys.argv[1:]

    if len(args) != 1:
        log.error("run.usage", message="usage: python -m news_archive.collectors.run <slug>")
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

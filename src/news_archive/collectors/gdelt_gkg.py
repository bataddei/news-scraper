"""GDELT GKG (Global Knowledge Graph) collector — filtered ingest.

GDELT v2 publishes a GKG file every 15 minutes at
`http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip`. Each file
contains ~1–2k rows, each row being one news article's structured metadata —
themes, tones, persons, organizations, locations. GDELT does NOT carry the
article text; it's a knowledge graph about publicly-indexed news.

Per the brief's filter mandate, we do NOT ingest every GDELT row (tens of
millions/year unfiltered). We keep rows matching ANY of:

    * A theme starting with any of the macro/policy prefixes
      (ECON_, WB_, EPU_, TAX_FNCACT_FED*, FISCAL_, INFLATION, INTEREST*,
      MONETARY, UNEMPLOYMENT, EMPLOYMENT*).
    * An exact theme from a curated allowlist (FOMC, FEDERAL_RESERVE, etc.).
    * A Mag 7 company-name substring in the URL or Organizations column.

The filter is intentionally conservative on the "cast a wide net for
macro-relevant news" side and restrictive on consumer/entertainment noise.
The filter predicate is recorded in `collection_runs.notes` so we can re-run
with a broader net later if needed (brief, §Week 3, GDELT note).
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from typing import Any

import httpx

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Ordered column names for the GDELT v2 GKG file (tab-separated, no header).
GKG_COLUMNS = (
    "GKGRECORDID",
    "V21DATE",
    "V2SOURCECOLLECTIONIDENTIFIER",
    "V2SOURCECOMMONNAME",
    "V2DOCUMENTIDENTIFIER",
    "V1COUNTS",
    "V21COUNTS",
    "V1THEMES",
    "V2ENHANCEDTHEMES",
    "V1LOCATIONS",
    "V2ENHANCEDLOCATIONS",
    "V1PERSONS",
    "V2ENHANCEDPERSONS",
    "V1ORGANIZATIONS",
    "V2ENHANCEDORGANIZATIONS",
    "V15TONE",
    "V21ENHANCEDDATES",
    "V2GCAM",
    "V21SHARINGIMAGE",
    "V21RELATEDIMAGES",
    "V21SOCIALIMAGEEMBEDS",
    "V21SOCIALVIDEOEMBEDS",
    "V21QUOTATIONS",
    "V21ALLNAMES",
    "V21AMOUNTS",
    "V21TRANSLATIONINFO",
    "V2EXTRASXML",
)

# Theme-prefix allowlist: rows whose V1THEMES contains ANY theme starting with
# one of these prefixes are kept.
_THEME_PREFIXES: tuple[str, ...] = (
    "ECON_",
    "WB_",
    "EPU_",
    "FISCAL_",
    "MONETARY",
    "INTEREST_RATE",
    "INFLATION",
    "UNEMPLOYMENT",
    "EMPLOYMENT_",
)

# Exact-theme allowlist — narrow, named themes that don't share a common prefix.
_THEME_EXACT: frozenset[str] = frozenset(
    {
        "FOMC",
        "FEDERAL_RESERVE",
        "CENTRAL_BANK",
        "CENTRAL_BANKS",
    }
)

# Mag 7 substrings searched against URL + V2ENHANCEDORGANIZATIONS.
# Lowercased before comparison.
_MAG7_NAME_SUBSTRINGS: tuple[str, ...] = (
    "apple inc",
    "microsoft",
    "alphabet",
    "google",
    "amazon.com",
    "meta platforms",
    "facebook",
    "nvidia",
    "tesla",
)
_MAG7_TICKER_FOR_NAME: dict[str, str] = {
    "apple inc": "AAPL",
    "microsoft": "MSFT",
    "alphabet": "GOOGL",
    "google": "GOOGL",
    "amazon.com": "AMZN",
    "meta platforms": "META",
    "facebook": "META",
    "nvidia": "NVDA",
    "tesla": "TSLA",
}


def pick_latest_gkg_url(lastupdate_body: str) -> str | None:
    """Parse the 3-line lastupdate.txt and return the gkg.csv.zip URL."""
    for line in lastupdate_body.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and parts[2].endswith(".gkg.csv.zip"):
            return parts[2]
    return None


def parse_v21_date(date_str: str | None) -> datetime | None:
    """YYYYMMDDHHMMSS → tz-aware UTC datetime."""
    if not date_str or len(date_str) != 14 or not date_str.isdigit():
        return None
    try:
        return datetime(
            int(date_str[0:4]),
            int(date_str[4:6]),
            int(date_str[6:8]),
            int(date_str[8:10]),
            int(date_str[10:12]),
            int(date_str[12:14]),
            tzinfo=UTC,
        )
    except ValueError:
        return None


def _themes_from_cell(cell: str | None) -> list[str]:
    """V1THEMES is ';'-separated list of theme tokens. Strip empties."""
    if not cell:
        return []
    return [t.strip() for t in cell.split(";") if t.strip()]


def theme_match(themes: list[str]) -> bool:
    for t in themes:
        if t in _THEME_EXACT:
            return True
        for prefix in _THEME_PREFIXES:
            if t.startswith(prefix):
                return True
    return False


def extract_mag7_tickers(url: str, orgs_cell: str | None) -> list[str]:
    """Return Mag 7 tickers whose company-name appears in URL or Organizations."""
    haystack = f"{url or ''} {orgs_cell or ''}".lower()
    out: list[str] = []
    seen: set[str] = set()
    for substring in _MAG7_NAME_SUBSTRINGS:
        if substring in haystack:
            ticker = _MAG7_TICKER_FOR_NAME[substring]
            if ticker not in seen:
                seen.add(ticker)
                out.append(ticker)
    return out


def row_passes_filter(row: dict[str, Any]) -> tuple[bool, str]:
    """Return (keep?, reason_tag) — reason is 'theme', 'mag7', or ''."""
    themes = _themes_from_cell(row.get("V1THEMES"))
    if theme_match(themes):
        return True, "theme"
    url = row.get("V2DOCUMENTIDENTIFIER") or ""
    orgs = row.get("V2ENHANCEDORGANIZATIONS") or ""
    if extract_mag7_tickers(url, orgs):
        return True, "mag7"
    return False, ""


def _iter_gkg_rows(csv_bytes: bytes) -> Iterator[dict[str, Any]]:
    """Parse tab-separated GKG CSV into column-named dicts. Streams lines."""
    buf = io.StringIO(csv_bytes.decode("utf-8", errors="replace"))
    # GKG fields can contain quotes and embedded tabs are avoided by escaping,
    # but the file uses literal TAB as delimiter with NO quoting scheme. Use
    # csv.reader with tab delimiter and QUOTE_NONE.
    reader = csv.reader(buf, delimiter="\t", quoting=csv.QUOTE_NONE)
    for cols in reader:
        if not cols:
            continue
        # Pad or truncate to the expected column count so we always index safely.
        if len(cols) < len(GKG_COLUMNS):
            cols = cols + [""] * (len(GKG_COLUMNS) - len(cols))
        elif len(cols) > len(GKG_COLUMNS):
            cols = cols[: len(GKG_COLUMNS)]
        yield dict(zip(GKG_COLUMNS, cols, strict=True))


def _summarise_themes(themes: list[str], limit: int = 8) -> str:
    return ", ".join(themes[:limit])


class GdeltGkgCollector(BaseCollector):
    source_slug = "gdelt_gkg"
    lastupdate_url = LASTUPDATE_URL

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        lastupdate_body = http.fetch_bytes(self.lastupdate_url).decode(
            "utf-8", errors="replace"
        )
        gkg_url = pick_latest_gkg_url(lastupdate_body)
        if gkg_url is None:
            self.logger.warning("gkg.no_url_in_lastupdate", body=lastupdate_body[:200])
            return

        self.logger.info("gkg.downloading", url=gkg_url)
        try:
            zipped = http.fetch_bytes(gkg_url)
        except httpx.HTTPStatusError as e:
            # GDELT race: lastupdate.txt sometimes points to a file that isn't
            # fully uploaded yet (404 for a few seconds). Skip this cycle; the
            # next cron tick will find the next file ready.
            if e.response.status_code == 404:
                self.logger.warning("gkg.url_not_ready", url=gkg_url)
                return
            raise
        fetched_at = utcnow()

        try:
            with zipfile.ZipFile(io.BytesIO(zipped)) as z:
                member_names = z.namelist()
                if not member_names:
                    self.logger.warning("gkg.empty_zip", url=gkg_url)
                    return
                with z.open(member_names[0]) as inner:
                    csv_bytes = inner.read()
        except zipfile.BadZipFile as e:
            self.logger.warning("gkg.bad_zip", url=gkg_url, error=str(e))
            return

        total = 0
        kept = 0
        for row in _iter_gkg_rows(csv_bytes):
            total += 1
            keep, reason = row_passes_filter(row)
            if not keep:
                continue

            url = row.get("V2DOCUMENTIDENTIFIER") or None
            rec_id = row.get("GKGRECORDID") or None
            published = parse_v21_date(row.get("V21DATE")) or fetched_at
            domain = row.get("V2SOURCECOMMONNAME") or ""
            themes = _themes_from_cell(row.get("V1THEMES"))
            tickers = extract_mag7_tickers(url or "", row.get("V2ENHANCEDORGANIZATIONS"))

            if not url and not rec_id:
                continue

            # GDELT has no headline — synthesise one so the articles table still
            # has a human-readable string to grep on. Theme-summary keeps it
            # backtest-useful.
            theme_tail = _summarise_themes(themes)
            headline = f"[{reason.upper()}] {domain} :: {theme_tail}".strip()
            if len(headline) > 400:
                headline = headline[:400]
            body = (
                f"url={url}\n"
                f"themes={';'.join(themes)}\n"
                f"orgs={row.get('V2ENHANCEDORGANIZATIONS') or ''}\n"
                f"persons={row.get('V2ENHANCEDPERSONS') or ''}\n"
                f"tone={row.get('V15TONE') or ''}"
            )

            raw_payload = {
                "_gdelt_file": gkg_url,
                "_filter_reason": reason,
                **{k: v for k, v in row.items() if v},
            }

            article = Article(
                source_id=self.source_id,
                external_id=rec_id,
                url=url,
                headline=headline,
                body=body,
                source_published_at=published,
                source_fetched_at=fetched_at,
                raw_payload=raw_payload,
                content_hash=content_hash(headline, body),
                language="en",
            )
            entities: list[ArticleEntity] = [
                ArticleEntity(entity_type="event", entity_value="GDELT"),
                ArticleEntity(entity_type="org", entity_value="GDELT Project"),
            ]
            for t in tickers:
                entities.append(ArticleEntity(entity_type="ticker", entity_value=t))
            yield article, entities
            kept += 1

        self.logger.info(
            "gkg.file_summary",
            url=gkg_url,
            total_rows=total,
            kept_rows=kept,
        )

"""GDELT GKG (Global Knowledge Graph) collector — 15-minute rollup ingest.

GDELT v2 publishes a GKG file every 15 minutes at
`http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip`. Each file
contains ~1–2k rows of structured metadata about news articles globally —
themes, tones, persons, organizations, locations.

We do NOT store every row. The "bigness" signal is mention volume, not any
single article. We aggregate per (15-min file, theme bucket) and write to
`news_archive.gdelt_rollup_15min`. See migration 0010 for the rationale and
look-ahead-safe daily view.

Filter is unchanged from the per-article era: keep rows matching ANY of
    * a theme prefix in `_THEME_PREFIXES`
    * an exact theme in `_THEME_EXACT`
    * a Mag 7 company-name substring in URL or organizations
"""

from __future__ import annotations

import csv
import io
import traceback
import zipfile
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import httpx

from news_archive import db, http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.models import Article, ArticleEntity, CollectionRun, GdeltRollup

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


def parse_window_start_from_url(gkg_url: str) -> datetime | None:
    """Pull the 15-min window start from `…/YYYYMMDDHHMMSS.gkg.csv.zip`.

    Used as `window_start` for every rollup row from this file so all rows
    share the same primary-key prefix and re-runs are idempotent.
    """
    leaf = urlparse(gkg_url).path.rsplit("/", 1)[-1]
    stem = leaf.split(".", 1)[0]
    return parse_v21_date(stem)


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


def buckets_for_row(themes: list[str], mag7_tickers: list[str]) -> set[str]:
    """Map a matched row to one or more rollup buckets.

    A single row contributes to every bucket it touches: e.g. a row about
    Apple-Inc and an FOMC decision counts in both `MAG7_AAPL` and `FOMC`.
    """
    out: set[str] = set()
    for ticker in mag7_tickers:
        out.add(f"MAG7_{ticker}")

    if any(t in _THEME_EXACT for t in themes):
        out.add("FOMC")
    if any(t.startswith("INFLATION") for t in themes):
        out.add("INFLATION")
    if any(t.startswith("INTEREST_RATE") for t in themes):
        out.add("INTEREST_RATE")
    if any(t.startswith("MONETARY") for t in themes):
        out.add("MONETARY")
    if any(t.startswith("EMPLOYMENT_") or t.startswith("UNEMPLOYMENT") for t in themes):
        out.add("EMPLOYMENT")
    if any(t.startswith("FISCAL_") for t in themes):
        out.add("FISCAL")
    if any(t.startswith("EPU_") for t in themes):
        out.add("EPU")
    if any(t.startswith("WB_") for t in themes):
        out.add("WB")
    if any(t.startswith("ECON_") for t in themes):
        out.add("ECON")
    return out


def parse_overall_tone(tone_cell: str | None) -> float | None:
    """V15TONE is `tone,positive,negative,polarity,activity,group_ref,word_count`.

    Position 0 is the overall tone score (positive - negative). Returns None
    if the cell is empty or unparseable.
    """
    if not tone_cell:
        return None
    head = tone_cell.split(",", 1)[0]
    try:
        return float(head)
    except ValueError:
        return None


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


@dataclass
class _BucketAcc:
    """Per-bucket accumulator built up while scanning one GKG file."""

    n_articles: int = 0
    domain_counts: Counter[str] = field(default_factory=Counter)
    tones: list[float] = field(default_factory=list)
    first_url: str | None = None

    def add(self, *, domain: str | None, url: str | None, tone: float | None) -> None:
        self.n_articles += 1
        if domain:
            self.domain_counts[domain] += 1
        if tone is not None:
            self.tones.append(tone)
        if self.first_url is None and url:
            self.first_url = url

    def to_rollup(
        self,
        *,
        window_start: datetime,
        fetched_at: datetime,
        theme_bucket: str,
    ) -> GdeltRollup:
        avg = sum(self.tones) / len(self.tones) if self.tones else None
        top_domain, _ = self.domain_counts.most_common(1)[0] if self.domain_counts else (None, 0)
        return GdeltRollup(
            window_start=window_start,
            fetched_at=fetched_at,
            theme_bucket=theme_bucket,
            n_articles=self.n_articles,
            n_sources=len(self.domain_counts),
            avg_tone=avg,
            min_tone=min(self.tones) if self.tones else None,
            max_tone=max(self.tones) if self.tones else None,
            top_url=self.first_url,
            top_domain=top_domain,
        )


def compute_rollups(
    rows: Iterable[dict[str, Any]],
    *,
    window_start: datetime,
    fetched_at: datetime,
) -> tuple[list[GdeltRollup], int, int]:
    """Walk all parsed GKG rows and return rollups + total/kept counts."""
    accs: dict[str, _BucketAcc] = {}
    total = 0
    kept = 0
    for row in rows:
        total += 1
        keep, _ = row_passes_filter(row)
        if not keep:
            continue
        kept += 1

        url = row.get("V2DOCUMENTIDENTIFIER") or None
        domain = row.get("V2SOURCECOMMONNAME") or None
        themes = _themes_from_cell(row.get("V1THEMES"))
        tickers = extract_mag7_tickers(url or "", row.get("V2ENHANCEDORGANIZATIONS"))
        tone = parse_overall_tone(row.get("V15TONE"))

        for bucket in buckets_for_row(themes, tickers):
            acc = accs.setdefault(bucket, _BucketAcc())
            acc.add(domain=domain, url=url, tone=tone)

    rollups = [
        accs[bucket].to_rollup(
            window_start=window_start,
            fetched_at=fetched_at,
            theme_bucket=bucket,
        )
        for bucket in sorted(accs)
    ]
    return rollups, total, kept


class GdeltGkgCollector(BaseCollector):
    """Collector for GDELT GKG → news_archive.gdelt_rollup_15min.

    Bypasses BaseCollector.run() because the rollup target schema is
    different from `articles`. `collect()` exists only to satisfy the
    abstract method; the real work is in `run()`.
    """

    source_slug = "gdelt_gkg"
    lastupdate_url = LASTUPDATE_URL

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        """Unused — `run()` is overridden. Returns an empty iterator."""
        return iter(())

    def run(self, notes: str | None = None) -> CollectionRun:
        run_record = CollectionRun(
            source_id=self.source_id,
            started_at=utcnow(),
            notes=notes,
        )
        run_record.id = db.start_collection_run(run_record)
        run_log = self.logger.bind(run_id=run_record.id)
        run_log.info("collection_run.start")

        try:
            outcome = self._ingest_one_file(run_log)
            if outcome is None:
                # No GKG file was available this cycle (e.g. lastupdate not
                # yet pointing at a fresh URL, or a 404 race). Treat as a
                # successful no-op so the gap report doesn't flag it.
                run_record.status = "success"
                run_record.articles_seen = 0
                run_record.articles_inserted = 0
            else:
                rollups_emitted, kept, total, inserted, duplicate = outcome
                run_record.articles_seen = kept
                run_record.articles_inserted = inserted
                run_record.articles_duplicate = duplicate
                run_record.status = "success"
                run_log.info(
                    "gkg.rollup_summary",
                    rollups=rollups_emitted,
                    kept_rows=kept,
                    total_rows=total,
                    inserted_buckets=inserted,
                    duplicate_buckets=duplicate,
                )
        except Exception as e:
            run_record.status = "failed"
            run_record.error_message = (
                f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            )
            run_log.error("collection_run.failed", error=str(e))
        finally:
            run_record.finished_at = utcnow()
            db.finish_collection_run(run_record)
            run_log.info(
                "collection_run.finish",
                status=run_record.status,
                seen=run_record.articles_seen,
                inserted=run_record.articles_inserted,
                duplicate=run_record.articles_duplicate,
            )
        return run_record

    def _ingest_one_file(
        self, run_log: Any
    ) -> tuple[int, int, int, int, int] | None:
        """Download, parse, aggregate, and insert one GKG file.

        Returns (n_rollups, kept_rows, total_rows, inserted_buckets,
        duplicate_buckets) or None when there's nothing to ingest this cycle.
        """
        lastupdate_body = http.fetch_bytes(self.lastupdate_url).decode(
            "utf-8", errors="replace"
        )
        gkg_url = pick_latest_gkg_url(lastupdate_body)
        if gkg_url is None:
            run_log.warning("gkg.no_url_in_lastupdate", body=lastupdate_body[:200])
            return None

        window_start = parse_window_start_from_url(gkg_url)
        if window_start is None:
            run_log.warning("gkg.window_unparseable", url=gkg_url)
            return None

        run_log.info("gkg.downloading", url=gkg_url, window_start=str(window_start))
        try:
            zipped = http.fetch_bytes(gkg_url)
        except httpx.HTTPStatusError as e:
            # GDELT race: lastupdate.txt sometimes points to a file that isn't
            # fully uploaded yet (404 for a few seconds). Skip this cycle; the
            # next cron tick will find the next file ready.
            if e.response.status_code == 404:
                run_log.warning("gkg.url_not_ready", url=gkg_url)
                return None
            raise
        fetched_at = utcnow()

        try:
            with zipfile.ZipFile(io.BytesIO(zipped)) as z:
                member_names = z.namelist()
                if not member_names:
                    run_log.warning("gkg.empty_zip", url=gkg_url)
                    return None
                with z.open(member_names[0]) as inner:
                    csv_bytes = inner.read()
        except zipfile.BadZipFile as e:
            run_log.warning("gkg.bad_zip", url=gkg_url, error=str(e))
            return None

        rollups, total, kept = compute_rollups(
            _iter_gkg_rows(csv_bytes),
            window_start=window_start,
            fetched_at=fetched_at,
        )
        inserted, duplicate = db.insert_gdelt_rollups(rollups)
        return len(rollups), kept, total, inserted, duplicate

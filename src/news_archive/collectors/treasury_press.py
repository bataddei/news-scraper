"""Treasury press releases collector — scrapes the Drupal listing page.

Treasury does not publish a dedicated RSS feed for press releases (the site-wide
`rss.xml` is a mixed-content stream that omits most press items). The listing at
`home.treasury.gov/news/press-releases` renders the full recent history server-side
with stable HTML, so scraping it is both reliable and low-bandwidth.

Listing row shape (repeating pattern inside the listing body):

    <div>
      <span class="date-format"><time datetime="ISO">April 19, 2026</time></span>
      <span>
        <span class="subcategory"><a ...>Readouts</a></span>
      </span>
      <h3 class="featured-stories__headline"><a href="/news/press-releases/sb0462">TITLE</a></h3>
    </div>

Only the `<h3 class="featured-stories__headline">` uniquely marks a listing row —
the mega-menu preview at the top of the page uses a different class, so this
selector also avoids double-counting.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

LISTING_URL = "https://home.treasury.gov/news/press-releases"
BASE = "https://home.treasury.gov"

SECTION_SLUGS = {
    "statements-remarks",
    "readouts",
    "testimonies",
    "reports",
    "feature",
    "featured-stories",
}


def _parse_listing(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, Any]] = []
    for h3 in soup.select("h3.featured-stories__headline"):
        a = h3.find("a")
        if a is None:
            continue
        href = (a.get("href") or "").strip()
        if not href.startswith("/news/press-releases/"):
            continue
        slug = href.rstrip("/").rsplit("/", 1)[-1]
        if slug in SECTION_SLUGS:
            continue

        parent = h3.parent
        time_tag = parent.find("time") if parent else None
        dt_attr = time_tag.get("datetime") if time_tag else None
        if not dt_attr:
            continue
        try:
            published = datetime.fromisoformat(str(dt_attr).replace("Z", "+00:00"))
        except ValueError:
            continue

        subcat_a = parent.select_one(".subcategory a") if parent else None
        subcategory = subcat_a.get_text(strip=True) if subcat_a else None

        rows.append(
            {
                "slug": slug,
                "url": BASE + href,
                "title": a.get_text(strip=True),
                "published": published,
                "subcategory": subcategory,
            }
        )
    return rows


class TreasuryPressCollector(BaseCollector):
    source_slug = "treasury_press"
    listing_url = LISTING_URL

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        raw = http.fetch_bytes(self.listing_url)
        fetched_at = utcnow()
        html = raw.decode("utf-8", errors="replace")

        entries = _parse_listing(html)
        self.logger.info(
            "listing.parsed",
            listing_url=self.listing_url,
            entries=len(entries),
        )

        for e in entries:
            headline = (e["title"] or "").strip()
            if not headline:
                self.logger.warning("item.skipped_no_headline", slug=e["slug"])
                continue

            raw_payload = {
                "_source_listing": self.listing_url,
                "slug": e["slug"],
                "url": e["url"],
                "title": headline,
                "published_iso": e["published"].isoformat(),
                "subcategory": e["subcategory"],
            }

            article = Article(
                source_id=self.source_id,
                external_id=e["slug"],
                url=e["url"],
                headline=headline,
                body=None,
                source_published_at=e["published"],
                source_fetched_at=fetched_at,
                raw_payload=raw_payload,
                content_hash=content_hash(headline, None),
                language="en",
            )
            entities = [
                ArticleEntity(entity_type="event", entity_value="TreasuryPress"),
                ArticleEntity(entity_type="org", entity_value="Treasury"),
            ]
            if e["subcategory"]:
                # Use release_type (same column BLS uses for CPI/PPI/etc.) so the
                # schema's CHECK constraint accepts it and Treasury sub-types are
                # queryable with the same predicate as BLS indicator filters.
                entities.append(
                    ArticleEntity(entity_type="release_type", entity_value=e["subcategory"])
                )
            yield article, entities

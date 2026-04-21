"""ForexFactory economic calendar collector.

ForexFactory itself blocks non-browser traffic on `forexfactory.com/*`, but the
community mirror `nfs.faireconomy.media/ff_calendar_thisweek.xml` publishes the
same this-week calendar as plain XML with stable fields. We ingest that.

Each `<event>` is stored as one row in `articles` even though it's metadata
rather than a news story — keeping one storage shape across all sources
simplifies backtest joins. The brief's intent is that these rows exist so a
later enrichment step can tag actual news articles to specific scheduled
releases.

Schema fit:
    headline                "CPI m/m (USD)"
    body                    "Impact: High | Forecast: 1.1% | Previous: 0.5%"
    source_published_at     event's scheduled time, parsed as US/Eastern → UTC
    source_fetched_at       now
    external_id             f"{url_slug}:{date}" — unique per event per day
    url                     the ff event URL
    raw_payload             full parsed event dict, including the original
                            (date, time, impact, forecast, previous) strings
                            so a future re-interpretation can fix tz or
                            impact-mapping issues without refetching.

Entities:
    event        = "EconCalendar"
    org          = "ForexFactory"
    release_type = <event title>  (e.g. "CPI m/m", "Non-Farm Employment Change")

Timezone note: the XML doesn't declare a tz. Empirically the mirror emits
US/Eastern times (DST-aware). We assume ET and convert to UTC; raw strings
are kept in raw_payload in case that assumption needs revisiting later.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, cast
from xml.etree import ElementTree as ET

try:
    from zoneinfo import ZoneInfo  # stdlib on py3.12
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

from news_archive import http
from news_archive.collectors.base import BaseCollector, utcnow
from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity

FEED_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.xml"
WIRE_NAME = "ForexFactory"
_ET_ZONE = ZoneInfo("America/New_York")

# Matches the final numeric prefix of a ForexFactory calendar slug:
#   "https://www.forexfactory.com/calendar/80-ca-cpi-mm" → slug="80-ca-cpi-mm"
_SLUG_RE = re.compile(r"/calendar/([\w-]+)/?$")

_NON_TIME_MARKERS = {"", "all day", "tentative", "day 1", "day 2", "day 3"}


def _extract_slug(url: str | None) -> str | None:
    if not url:
        return None
    m = _SLUG_RE.search(url)
    return m.group(1) if m else None


def parse_ff_datetime(date_str: str | None, time_str: str | None) -> datetime | None:
    """Turn ForexFactory (MM-DD-YYYY, "10:45pm") into tz-aware UTC.

    Returns the ET-midnight equivalent (converted to UTC) when the time field
    is absent / "All Day" / "Tentative" — we still know the day, just not the
    minute, and day-level fidelity is better than dropping the event.
    """
    if not date_str:
        return None
    date_str = date_str.strip()
    try:
        date_only = datetime.strptime(date_str, "%m-%d-%Y")
    except ValueError:
        return None

    normalized = (time_str or "").strip().lower()
    if normalized in _NON_TIME_MARKERS:
        return date_only.replace(tzinfo=_ET_ZONE).astimezone(UTC)

    try:
        dt = datetime.strptime(f"{date_str} {normalized}", "%m-%d-%Y %I:%M%p")
    except ValueError:
        return date_only.replace(tzinfo=_ET_ZONE).astimezone(UTC)
    return dt.replace(tzinfo=_ET_ZONE).astimezone(UTC)


def _event_text(event: ET.Element, tag: str) -> str | None:
    el = event.find(tag)
    if el is None:
        return None
    val = el.text
    return val.strip() if val else None


def parse_weekly_events(xml_bytes: bytes) -> list[dict[str, Any]]:
    """Parse the `<weeklyevents>` XML into normalized event dicts."""
    root = ET.fromstring(xml_bytes)
    out: list[dict[str, Any]] = []
    for ev in root.findall("event"):
        title = _event_text(ev, "title") or ""
        currency = _event_text(ev, "country") or ""
        date_str = _event_text(ev, "date")
        time_str = _event_text(ev, "time")
        impact = _event_text(ev, "impact")
        forecast = _event_text(ev, "forecast")
        previous = _event_text(ev, "previous")
        url = _event_text(ev, "url")

        scheduled = parse_ff_datetime(date_str, time_str)
        out.append(
            {
                "title": title,
                "currency": currency,
                "date": date_str,
                "time": time_str,
                "impact": impact,
                "forecast": forecast,
                "previous": previous,
                "url": url,
                "slug": _extract_slug(url),
                "scheduled_utc": scheduled,
            }
        )
    return out


def _build_body(event: dict[str, Any]) -> str | None:
    parts: list[str] = []
    if event.get("impact"):
        parts.append(f"Impact: {event['impact']}")
    if event.get("forecast"):
        parts.append(f"Forecast: {event['forecast']}")
    if event.get("previous"):
        parts.append(f"Previous: {event['previous']}")
    return " | ".join(parts) if parts else None


class ForexFactoryCalendarCollector(BaseCollector):
    source_slug = "econ_calendar_ff"
    feed_url = FEED_URL

    def collect(self) -> Iterable[tuple[Article, list[ArticleEntity]]]:
        raw = http.fetch_bytes(self.feed_url)
        fetched_at = utcnow()

        try:
            events = parse_weekly_events(raw)
        except ET.ParseError as e:
            self.logger.warning("feed.parse_failed", error=str(e))
            return

        self.logger.info("feed.loaded", feed_url=self.feed_url, events=len(events))

        for e in events:
            title = e["title"]
            currency = e["currency"]
            scheduled = cast(datetime | None, e.get("scheduled_utc"))
            slug = e.get("slug")

            if not title or not currency:
                self.logger.warning(
                    "item.skipped_missing_field",
                    title=title,
                    currency=currency,
                )
                continue
            if scheduled is None:
                self.logger.warning(
                    "item.skipped_unparseable_date",
                    title=title,
                    date=e.get("date"),
                    time=e.get("time"),
                )
                continue

            headline = f"{title} ({currency})"
            body = _build_body(e)
            # A ForexFactory event can recur across months with the same slug,
            # so compose external_id from slug + the event's date to stay unique.
            external_id = f"{slug}:{e.get('date')}" if slug else f"{title}:{e.get('date')}"

            raw_payload = {
                "_source_feed": self.feed_url,
                **{k: v for k, v in e.items() if k != "scheduled_utc"},
                "scheduled_utc": scheduled.isoformat(),
            }

            article = Article(
                source_id=self.source_id,
                external_id=external_id,
                url=e.get("url"),
                headline=headline,
                body=body,
                source_published_at=scheduled,
                source_fetched_at=fetched_at,
                raw_payload=raw_payload,
                content_hash=content_hash(headline, body),
                language="en",
            )
            entities = [
                ArticleEntity(entity_type="event", entity_value="EconCalendar"),
                ArticleEntity(entity_type="org", entity_value=WIRE_NAME),
                ArticleEntity(entity_type="release_type", entity_value=title),
            ]
            yield article, entities

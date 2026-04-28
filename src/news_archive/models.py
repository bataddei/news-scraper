"""Pydantic models for rows written to Supabase.

Models validate at construction time so malformed data is caught inside the
collector rather than as a Postgres error 500 rows later. The three timestamps
are required on every article — no defaults that could hide a parsing bug.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Article(BaseModel):
    """One row destined for `news_archive.articles`."""

    model_config = ConfigDict(extra="forbid")

    source_id: int
    external_id: str | None = None
    url: str | None = None
    headline: str = Field(min_length=1)
    body: str | None = None
    author: str | None = None
    source_published_at: datetime
    source_fetched_at: datetime
    raw_payload: dict[str, Any] | None = None
    content_hash: str = Field(min_length=64, max_length=64)
    language: str | None = Field(default=None, max_length=8)

    @field_validator("source_published_at", "source_fetched_at")
    @classmethod
    def _require_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC preferred)")
        return v


class ArticleEntity(BaseModel):
    """One row destined for `news_archive.article_entities`."""

    model_config = ConfigDict(extra="forbid")

    entity_type: str  # one of: ticker, person, org, event, release_type
    entity_value: str
    confidence: float | None = None


class GdeltRollup(BaseModel):
    """One row destined for `news_archive.gdelt_rollup_15min`.

    Aggregates all matched GDELT GKG rows in a single 15-min file that
    belong to one theme/mag7 bucket.
    """

    model_config = ConfigDict(extra="forbid")

    window_start: datetime
    fetched_at: datetime
    theme_bucket: str = Field(min_length=1, max_length=64)
    n_articles: int = Field(ge=1)
    n_sources: int = Field(ge=1)
    avg_tone: float | None = None
    min_tone: float | None = None
    max_tone: float | None = None
    top_url: str | None = None
    top_domain: str | None = None

    @field_validator("window_start", "fetched_at")
    @classmethod
    def _require_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC preferred)")
        return v


class CollectionRun(BaseModel):
    """In-memory view of a `news_archive.collection_runs` row during a collector invocation."""

    model_config = ConfigDict(extra="forbid")

    id: int | None = None
    source_id: int
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"  # running | success | partial | failed
    articles_seen: int = 0
    articles_inserted: int = 0
    articles_duplicate: int = 0
    error_message: str | None = None
    notes: str | None = None

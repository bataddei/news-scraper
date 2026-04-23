"""Pydantic models for rows written to the `literature` schema.

Parallel to `news_archive.models`. Validates at construction time so malformed
data is caught inside the collector rather than as a Postgres error. The three
timestamps are required on every paper — no defaults that could hide a parsing bug.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Paper(BaseModel):
    """One row destined for `literature.papers`."""

    model_config = ConfigDict(extra="forbid")

    source_id: int
    external_id: str | None = None
    url: str = Field(min_length=1)
    pdf_url: str | None = None
    title: str = Field(min_length=1)
    authors: list[str] = Field(default_factory=list)
    abstract: str | None = None
    categories: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    source_published_at: datetime
    source_fetched_at: datetime
    raw_payload: dict[str, Any] | None = None
    content_hash: str = Field(min_length=64, max_length=64)

    @field_validator("source_published_at", "source_fetched_at")
    @classmethod
    def _require_tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC preferred)")
        return v


class TriageRecord(BaseModel):
    """One row destined for `literature.triage`.

    Re-triaging the same paper with a new prompt inserts a row with a new
    `triage_version` — never an UPDATE.
    """

    model_config = ConfigDict(extra="forbid")

    paper_id: int
    triage_version: str = Field(min_length=1)
    model_used: str = Field(min_length=1)
    score_systematic_futures: int = Field(ge=0, le=10)
    score_short_timeframe: int = Field(ge=0, le=10)
    score_empirical_rigor: int = Field(ge=0, le=10)
    score_data_accessibility: int = Field(ge=0, le=10)
    score_implementation_effort: int = Field(ge=0, le=10)
    overall_priority: int = Field(ge=0, le=10)
    claimed_edge: str | None = None
    required_data: str | None = None
    method_summary: str | None = None
    red_flags: str | None = None
    reasoning: str | None = None
    raw_response: dict[str, Any] | None = None


class LitCollectionRun(BaseModel):
    """In-memory view of a `literature.collection_runs` row during a collector invocation.

    Field names mirror `news_archive.models.CollectionRun` so the base-class
    bookkeeping stays consistent across pipelines.
    """

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

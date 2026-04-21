from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from news_archive.hashing import content_hash
from news_archive.models import Article, ArticleEntity, CollectionRun


def _valid_article_kwargs() -> dict:
    return {
        "source_id": 1,
        "headline": "Fed holds rates steady",
        "body": "Body text.",
        "source_published_at": datetime(2024, 6, 1, 12, 30, tzinfo=UTC),
        "source_fetched_at": datetime(2024, 6, 1, 12, 31, tzinfo=UTC),
        "content_hash": content_hash("Fed holds rates steady", "Body text."),
    }


class TestArticle:
    def test_minimal_valid(self) -> None:
        article = Article(**_valid_article_kwargs())
        assert article.source_id == 1
        assert article.source_published_at.tzinfo is not None

    def test_rejects_naive_timestamp(self) -> None:
        kwargs = _valid_article_kwargs()
        kwargs["source_fetched_at"] = datetime(2024, 6, 1, 12, 31)  # no tzinfo
        with pytest.raises(ValidationError, match="timezone-aware"):
            Article(**kwargs)

    def test_rejects_empty_headline(self) -> None:
        kwargs = _valid_article_kwargs()
        kwargs["headline"] = ""
        with pytest.raises(ValidationError):
            Article(**kwargs)

    def test_rejects_short_content_hash(self) -> None:
        kwargs = _valid_article_kwargs()
        kwargs["content_hash"] = "tooshort"
        with pytest.raises(ValidationError):
            Article(**kwargs)

    def test_rejects_unknown_field(self) -> None:
        kwargs = _valid_article_kwargs()
        kwargs["not_a_column"] = "whoops"
        with pytest.raises(ValidationError):
            Article(**kwargs)


class TestArticleEntity:
    def test_minimal_valid(self) -> None:
        e = ArticleEntity(entity_type="ticker", entity_value="AAPL")
        assert e.confidence is None


class TestCollectionRun:
    def test_defaults_to_running_with_zero_counters(self) -> None:
        run = CollectionRun(source_id=1, started_at=datetime.now(UTC))
        assert run.status == "running"
        assert run.articles_seen == 0
        assert run.articles_inserted == 0
        assert run.articles_duplicate == 0

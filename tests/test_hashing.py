from __future__ import annotations

from news_archive.hashing import content_hash, normalize


class TestNormalize:
    def test_collapses_whitespace(self) -> None:
        assert normalize("hello   world") == "hello world"

    def test_strips_leading_and_trailing(self) -> None:
        assert normalize("  hello\n") == "hello"

    def test_lowercases(self) -> None:
        assert normalize("FOMC Statement") == "fomc statement"

    def test_nfkc_unifies_compatibility_forms(self) -> None:
        # Half-width and full-width ASCII digits normalize to the same thing.
        assert normalize("ＡＢＣ") == normalize("ABC")


class TestContentHash:
    def test_deterministic(self) -> None:
        h1 = content_hash("Fed holds rates", "Full body text.")
        h2 = content_hash("Fed holds rates", "Full body text.")
        assert h1 == h2
        assert len(h1) == 64

    def test_differs_with_different_headline(self) -> None:
        assert content_hash("Fed holds rates", "body") != content_hash(
            "Fed raises rates", "body"
        )

    def test_differs_with_different_body(self) -> None:
        assert content_hash("Fed holds rates", "body A") != content_hash(
            "Fed holds rates", "body B"
        )

    def test_ignores_whitespace_and_case_changes(self) -> None:
        assert content_hash("FED HOLDS  rates", "Body.") == content_hash(
            "fed holds rates", "body."
        )

    def test_handles_none_body(self) -> None:
        # Headline-only feeds must hash deterministically.
        assert content_hash("FOMC statement", None) == content_hash("FOMC statement", "")

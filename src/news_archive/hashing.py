"""Content hashing for deduplication.

`content_hash` is the primary dedup key inside a source. Two rows with the same
`(source_id, content_hash)` are the same article. Normalization is deliberately
aggressive so whitespace and case churn from a re-scraped page don't create dupes.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata

_WHITESPACE_RE = re.compile(r"\s+")


def normalize(text: str) -> str:
    """NFKC-normalize, strip, collapse whitespace, lowercase."""
    text = unicodedata.normalize("NFKC", text)
    text = text.strip().lower()
    text = _WHITESPACE_RE.sub(" ", text)
    return text


def content_hash(headline: str, body: str | None) -> str:
    """SHA-256 of `normalize(headline) || "\\n" || normalize(body or "")`."""
    normalized = normalize(headline) + "\n" + normalize(body or "")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

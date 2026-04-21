"""Pytest config.

We stub SUPABASE_DB_URL before any `news_archive.*` import so modules that
validate settings at import time don't fail in an environment without a real .env
(e.g. CI, or a fresh clone before `.env` is filled in). Tests that need a real DB
connection must skip themselves if `SUPABASE_DB_URL` is not set to something real.
"""

from __future__ import annotations

import os

os.environ.setdefault(
    "SUPABASE_DB_URL",
    "postgresql://stub:stub@localhost:5432/stub",
)

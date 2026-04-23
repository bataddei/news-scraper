"""Literature archive — academic finance + quant research papers.

Parallel to `news_archive.collectors` / `news_archive.db`. Writes to the
`literature.*` Postgres schema. Shares `config`, `http`, `hashing`,
`logging_config` with the news pipeline since those are schema-agnostic.
"""

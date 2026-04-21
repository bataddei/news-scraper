-- 0002_indexes.sql
-- Indexes for dedup and typical backtest queries.
-- All created with IF NOT EXISTS so re-running is safe.

-- ---- articles ----

-- Dedup #1: a source's own IDs (e.g. EDGAR accession, GDELT GKGRECORDID).
-- Unique only where external_id is present (partial unique).
create unique index if not exists articles_source_external_id_uidx
    on news_archive.articles (source_id, external_id)
    where external_id is not null;

-- Dedup #2: the primary mechanism — SHA-256 of normalized content.
-- Unique per source. Cross-source duplicates are kept on purpose.
create unique index if not exists articles_source_content_hash_uidx
    on news_archive.articles (source_id, content_hash);

-- Backtest query accelerators.
create index if not exists articles_source_published_at_idx
    on news_archive.articles (source_published_at);

create index if not exists articles_source_id_published_at_idx
    on news_archive.articles (source_id, source_published_at);

-- ---- article_entities ----

create index if not exists article_entities_type_value_idx
    on news_archive.article_entities (entity_type, entity_value);

create index if not exists article_entities_article_id_idx
    on news_archive.article_entities (article_id);

-- ---- collection_runs ----

-- Gap detection walks this index daily to check each source's last success.
create index if not exists collection_runs_source_started_at_idx
    on news_archive.collection_runs (source_id, started_at desc);

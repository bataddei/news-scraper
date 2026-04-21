-- 0001_init_schema.sql
-- Creates the `news_archive` schema and the four core tables.
-- Schema is append-only: never drop or destructively alter columns. All future changes
-- must be additive (new nullable columns, new tables, new indexes).

create schema if not exists news_archive;

-- ---------------------------------------------------------------------------
-- sources: catalog of every place we collect from. Seeded in 0003.
-- ---------------------------------------------------------------------------
create table if not exists news_archive.sources (
    id           serial primary key,
    slug         text unique not null,
    name         text not null,
    tier         int  not null check (tier in (1, 2, 3)),
    source_type  text not null check (source_type in ('rss', 'api', 'scraper', 'bulk')),
    base_url     text,
    active       boolean not null default true,
    notes        text,
    created_at   timestamptz not null default (now() at time zone 'utc')
);

comment on table news_archive.sources is
    'Catalog of every news source. Tier 1 = Fed/BLS/Treasury/SEC, Tier 2 = GDELT/wires, Tier 3 = future expansion.';

-- ---------------------------------------------------------------------------
-- articles: one row per unique article within a source.
-- Three timestamps are sacred:
--   source_published_at = when the source says it was published (parsed).
--   source_fetched_at   = when our collector got the HTTP response.
--   db_inserted_at      = when this row was written (DB default).
-- Backtest-available time = GREATEST(source_published_at, source_fetched_at).
-- ---------------------------------------------------------------------------
create table if not exists news_archive.articles (
    id                   bigserial primary key,
    source_id            int not null references news_archive.sources(id),
    external_id          text,
    url                  text,
    headline             text not null,
    body                 text,
    author               text,
    source_published_at  timestamptz not null,
    source_fetched_at    timestamptz not null,
    db_inserted_at       timestamptz not null default (now() at time zone 'utc'),
    raw_payload          jsonb,
    content_hash         text not null,
    language             text
);

comment on table news_archive.articles is
    'Every news item. Immutable after insert. See project-brief.md §Timestamp protocol.';

comment on column news_archive.articles.content_hash is
    'SHA-256 of normalized headline || body. Primary dedup key within a source.';

-- ---------------------------------------------------------------------------
-- article_entities: tagged entities per article. Tier 1 sources populate at
-- ingestion (e.g. EDGAR gives us the ticker). Tier 2 rows left empty for now;
-- an NLP backfill job will populate them later.
-- ---------------------------------------------------------------------------
create table if not exists news_archive.article_entities (
    id            bigserial primary key,
    article_id    bigint not null references news_archive.articles(id) on delete cascade,
    entity_type   text not null check (entity_type in ('ticker', 'person', 'org', 'event', 'release_type')),
    entity_value  text not null,
    confidence    numeric
);

comment on table news_archive.article_entities is
    'Entities tagged to articles. Populated at ingest for Tier 1, via NLP later for Tier 2.';

-- ---------------------------------------------------------------------------
-- collection_runs: operational log of every collector invocation.
-- Critical for the Week 4 gap-detection report.
-- ---------------------------------------------------------------------------
create table if not exists news_archive.collection_runs (
    id                  bigserial primary key,
    source_id           int not null references news_archive.sources(id),
    started_at          timestamptz not null,
    finished_at         timestamptz,
    status              text not null check (status in ('running', 'success', 'partial', 'failed')),
    articles_seen       int,
    articles_inserted   int,
    articles_duplicate  int,
    error_message       text,
    notes               text
);

comment on table news_archive.collection_runs is
    'One row per collector invocation. If finished_at is null long after started_at, the run crashed.';

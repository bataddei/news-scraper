-- 0005_literature_init_schema.sql
-- Creates the `literature` schema and its four core tables. Mirrors the shape
-- of `news_archive` (same timestamp protocol, same collection_runs layout) but
-- is fully isolated so the news pipeline is unaffected.
-- Schema is append-only: never drop or destructively alter columns. All future
-- changes must be additive (new nullable columns, new tables, new indexes).

create schema if not exists literature;

-- ---------------------------------------------------------------------------
-- sources: catalog of every place we collect papers from. Seeded in 0007.
-- Note the extra `feed_url` column (vs. news_archive.sources): it lets the
-- blog-RSS collector iterate rows to collect, so adding a new blog is an
-- INSERT migration rather than a code change.
-- ---------------------------------------------------------------------------
create table if not exists literature.sources (
    id           serial primary key,
    slug         text unique not null,
    name         text not null,
    tier         int  not null check (tier in (1, 2, 3)),
    source_type  text not null check (source_type in ('rss', 'api', 'scraper', 'bulk')),
    base_url     text,
    feed_url     text,
    active       boolean not null default true,
    notes        text,
    created_at   timestamptz not null default (now() at time zone 'utc')
);

comment on table literature.sources is
    'Catalog of every literature source. Tier 1 = arXiv/SSRN, Tier 2 = curated quant blogs, Tier 3 = journal RSS (abstracts + metadata only; bodies paywalled).';

-- ---------------------------------------------------------------------------
-- papers: one row per unique paper within a source.
-- Three timestamps are sacred (same protocol as news_archive.articles):
--   source_published_at = when the source says it was published (parsed).
--   source_fetched_at   = when our collector got the HTTP response.
--   db_inserted_at      = when this row was written (DB default).
-- ---------------------------------------------------------------------------
create table if not exists literature.papers (
    id                   bigserial primary key,
    source_id            int not null references literature.sources(id),
    external_id          text,
    url                  text not null,
    pdf_url              text,
    title                text not null,
    authors              text[] not null default '{}'::text[],
    abstract             text,
    categories           text[] not null default '{}'::text[],
    keywords             text[] not null default '{}'::text[],
    source_published_at  timestamptz not null,
    source_fetched_at    timestamptz not null,
    db_inserted_at       timestamptz not null default (now() at time zone 'utc'),
    raw_payload          jsonb,
    content_hash         text not null
);

comment on table literature.papers is
    'Every paper / post. Immutable after insert. See project-brief.md §Timestamp protocol.';

comment on column literature.papers.content_hash is
    'SHA-256 of normalized title || abstract. Primary dedup key within a source.';

-- ---------------------------------------------------------------------------
-- triage: one row per (paper, triage_version). Separate from papers so we can
-- re-triage with an improved prompt later without mutating the archive.
-- ---------------------------------------------------------------------------
create table if not exists literature.triage (
    id                           bigserial primary key,
    paper_id                     bigint not null references literature.papers(id) on delete cascade,
    triage_version               text not null,
    model_used                   text not null,
    score_systematic_futures     int  not null check (score_systematic_futures    between 0 and 10),
    score_short_timeframe        int  not null check (score_short_timeframe       between 0 and 10),
    score_empirical_rigor        int  not null check (score_empirical_rigor       between 0 and 10),
    score_data_accessibility     int  not null check (score_data_accessibility    between 0 and 10),
    score_implementation_effort  int  not null check (score_implementation_effort between 0 and 10),
    overall_priority             int  not null check (overall_priority            between 0 and 10),
    claimed_edge                 text,
    required_data                text,
    method_summary               text,
    red_flags                    text,
    reasoning                    text,
    raw_response                 jsonb,
    triaged_at                   timestamptz not null default (now() at time zone 'utc')
);

comment on table literature.triage is
    'One row per (paper, triage_version). Re-triaging with a new prompt creates a NEW row; never overwrite.';

-- ---------------------------------------------------------------------------
-- collection_runs: operational log of every collector invocation.
-- Mirror of news_archive.collection_runs; kept in its own schema for clean
-- gap-detection and so the news pipeline's reports aren't affected.
-- ---------------------------------------------------------------------------
create table if not exists literature.collection_runs (
    id                  bigserial primary key,
    source_id           int not null references literature.sources(id),
    started_at          timestamptz not null,
    finished_at         timestamptz,
    status              text not null check (status in ('running', 'success', 'partial', 'failed')),
    articles_seen       int,
    articles_inserted   int,
    articles_duplicate  int,
    error_message       text,
    notes               text
);

comment on table literature.collection_runs is
    'One row per collector invocation. If finished_at is null long after started_at, the run crashed.';

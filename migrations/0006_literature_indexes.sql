-- 0006_literature_indexes.sql
-- Dedup and query accelerators for the literature schema.
-- All created with IF NOT EXISTS so re-running is safe.

-- ---- papers ----

-- Dedup #1: a source's own id (arXiv id, SSRN abstract id, DOI, feed GUID).
-- Unique only where external_id is present (partial unique).
create unique index if not exists papers_source_external_id_uidx
    on literature.papers (source_id, external_id)
    where external_id is not null;

-- Dedup #2: the primary mechanism — SHA-256 of normalized title + abstract.
-- Unique per source; cross-source duplicates are kept on purpose (same idea
-- reported by two outlets is signal, not noise).
create unique index if not exists papers_source_content_hash_uidx
    on literature.papers (source_id, content_hash);

-- Backtest + discovery query accelerators.
create index if not exists papers_source_published_at_idx
    on literature.papers (source_published_at);

create index if not exists papers_source_id_published_at_idx
    on literature.papers (source_id, source_published_at);

-- ---- triage ----

-- Idempotency: re-running the triage job for the same (paper, version) is a no-op.
create unique index if not exists triage_paper_version_uidx
    on literature.triage (paper_id, triage_version);

-- "Give me today's newly-triaged papers" and "highest-priority recent triage rows".
create index if not exists triage_version_triaged_at_idx
    on literature.triage (triage_version, triaged_at desc);

create index if not exists triage_overall_priority_triaged_at_idx
    on literature.triage (overall_priority desc, triaged_at desc);

-- ---- collection_runs ----

-- Gap detection walks this index to check each source's last success.
create index if not exists lit_collection_runs_source_started_at_idx
    on literature.collection_runs (source_id, started_at desc);

-- 0010_gdelt_rollup_15min.sql
-- Replaces per-article GDELT ingestion with a 15-minute rollup.
--
-- Rationale: GDELT GKG is fire-hose data — ~3K matched rows/day at ~14 KB
-- raw_payload each. The "bigness" signal is mention volume, not any single
-- row, so we aggregate per (15-min file, theme bucket) instead of storing
-- every row.
--
-- Look-ahead-safe daily view: bucketing uses GREATEST(window_start,
-- fetched_at). If a scrape was delayed (cron crashed, recovered hours later)
-- we cannot claim to "have known" the news at the file's window_start —
-- only at fetched_at. The brief's timestamp protocol (§Timestamp protocol)
-- says backtest-available time = GREATEST(source_published_at,
-- source_fetched_at). Same principle applied here.

create table if not exists news_archive.gdelt_rollup_15min (
    window_start    timestamptz not null,         -- 15-min file timestamp (UTC)
    fetched_at      timestamptz not null,         -- when our collector got it (UTC)
    theme_bucket    text        not null,         -- e.g. FOMC, INFLATION, MAG7_AAPL
    n_articles      int         not null,         -- matched rows in this bucket
    n_sources       int         not null,         -- distinct domains in this bucket
    avg_tone        double precision,
    min_tone        double precision,
    max_tone        double precision,
    top_url         text,                          -- one example URL (first row)
    top_domain      text,                          -- most common V2SOURCECOMMONNAME
    primary key (window_start, theme_bucket)
);

comment on table news_archive.gdelt_rollup_15min is
    '15-min GDELT rollup. Replaces per-article GDELT rows. '
    'See migration 0010 header for rationale.';

create index if not exists gdelt_rollup_bucket_window_idx
    on news_archive.gdelt_rollup_15min (theme_bucket, window_start desc);

-- Daily 09:00 ET trading-day view. The trading day for a window is:
--   (D-1 09:00 ET, D 09:00 ET]  →  trading_day = D
-- So a 15:30 ET headline today rolls into tomorrow's premarket bucket; an
-- 04:00 ET headline today rolls into today's premarket bucket.
--
-- avg_tone is article-count-weighted across the windows in the day so a
-- noisy 3-article window doesn't dominate a 1000-article window.
create or replace view news_archive.gdelt_rollup_daily_09et as
with available as (
    select
        greatest(window_start, fetched_at) as available_at,
        theme_bucket,
        n_articles,
        n_sources,
        avg_tone,
        min_tone,
        max_tone
    from news_archive.gdelt_rollup_15min
),
local as (
    select
        (available_at at time zone 'America/New_York') as available_local,
        theme_bucket,
        n_articles,
        n_sources,
        avg_tone,
        min_tone,
        max_tone
    from available
)
select
    case
        when extract(hour from available_local) >= 9
            then (available_local + interval '1 day')::date
        else available_local::date
    end                                                as trading_day,
    theme_bucket,
    sum(n_articles)::int                               as n_articles,
    sum(n_sources)::int                                as n_sources_summed,
    sum(n_articles * avg_tone) / nullif(sum(n_articles), 0)
                                                       as avg_tone,
    min(min_tone)                                      as min_tone,
    max(max_tone)                                      as max_tone
from local
group by trading_day, theme_bucket;

comment on view news_archive.gdelt_rollup_daily_09et is
    'Trading-day rollup for premarket check. Trading day D = window '
    '(D-1 09:00 ET, D 09:00 ET]. n_sources_summed is approximate '
    '(sum of per-15min distinct counts, not true daily distinct).';

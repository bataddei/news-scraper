# News Archive Pipeline — Project Specification

## Context and purpose

This project builds a 24/7 news collection pipeline that archives financial news, Fed communications, economic releases, and SEC filings into a Supabase Postgres database. The archive is being built **now** so that in 6–12 months, when the operator has tick-level market data and is working on regime detection and confidence scoring for a systematic MNQ futures trading strategy, there is a proprietary news archive available to backtest against.

**The archive is the product.** The pipeline is infrastructure. Every design decision should be evaluated against one question: *"Will this data be trustworthy for backtesting in 12 months?"*

### Why this matters
- News data that isn't collected today can never be backtested later. Historical news APIs are expensive and incomplete.
- Timestamps must be rigorously captured — lookahead bias from bad timestamps would silently poison every future backtest.
- The archive must be append-only and immutable at the row level once written.

### What this project is NOT
- Not a real-time trading signal (the bot will not read from this DB initially).
- Not a news-reading UI for humans.
- Not a sentiment scoring service — sentiment/NLP comes later, we only store raw data now.

---

## Operator profile and working style

- The operator is **not a programmer**. All code will be written by Claude Code. The operator makes design decisions, reviews behavior, and runs the system.
- The operator already runs a Nautilus Trader bot on a DigitalOcean droplet in NY. **This project must not touch or risk that bot.**
- The operator has a working Supabase account.
- The operator works in bursts and wants a system that runs unattended for weeks at a time once deployed.

---

## Infrastructure

### Hosting
- **New DigitalOcean droplet, separate from the trading bot.**
- Smallest reasonable size (~$6/month, 1GB RAM, 1 vCPU, 25GB SSD). Can be resized later.
- Region: choose one close to Supabase region for low DB write latency. EU region likely fine; US East also acceptable.
- Ubuntu 24.04 LTS.

### Storage
- **Supabase (Postgres)** — operator's existing account.
- Use a dedicated schema (e.g. `news_archive`) to isolate from any other Supabase data.
- Connection via Supabase's connection pooler (transaction mode) for long-running scripts.
- Credentials stored in environment variables on the droplet, never committed to git.

### Source control
- Single git repo hosted on GitHub (private).
- Claude Code works directly in this repo.
- Secrets loaded from `.env` file on droplet, excluded via `.gitignore`.

### Process management
- **systemd services** for each long-running collector (one service per source or source group).
- **cron** for scheduled pulls (e.g. daily GDELT bulk download, hourly EDGAR checks).
- Logs written to `/var/log/news-pipeline/` with logrotate configured.

### Language and stack
- **Python 3.12.**
- Libraries: `httpx` (async HTTP), `feedparser` (RSS), `psycopg` v3 (Postgres), `pydantic` v2 (schema validation), `tenacity` (retries), `structlog` (structured logs).
- No heavy frameworks. Each collector is a small, focused script.

---

## Timestamp protocol — CRITICAL

The single most important part of this system. Every row stored **must** capture three timestamps:

1. `source_published_at` — when the source says the content was published. Parsed from RSS `<pubDate>`, API response fields, or HTML metadata. Timezone-aware, stored as UTC.
2. `source_fetched_at` — when our collector actually fetched the content. Set by the collector at the moment of successful HTTP response.
3. `db_inserted_at` — when the row was written to Postgres. Set by database default `NOW() AT TIME ZONE 'UTC'`.

**Backtesting rule:** When querying the archive for backtesting, use `GREATEST(source_published_at, source_fetched_at)` as the "available-to-trade" timestamp. This prevents lookahead bias in cases where a source backdates its publication time.

All three timestamps are stored; none are derived at query time.

---

## Schema design

One schema `news_archive` containing the following tables.

### `sources`
Catalog of every source we collect from. Populated by seed data at deploy time.

| column | type | notes |
|---|---|---|
| `id` | `serial primary key` | |
| `slug` | `text unique not null` | e.g. `fed_fomc_statements`, `bls_cpi`, `gdelt_gkg` |
| `name` | `text not null` | human-readable |
| `tier` | `int not null` | 1 = Fed/BLS/Treasury/SEC, 2 = GDELT/RSS wires |
| `source_type` | `text not null` | `rss`, `api`, `scraper`, `bulk` |
| `base_url` | `text` | |
| `active` | `boolean not null default true` | |
| `notes` | `text` | |

### `articles`
Every news item, one row per unique article.

| column | type | notes |
|---|---|---|
| `id` | `bigserial primary key` | |
| `source_id` | `int not null references sources(id)` | |
| `external_id` | `text` | source's own ID if available (e.g. EDGAR accession, GDELT GKGRECORDID) |
| `url` | `text` | canonical URL of the article |
| `headline` | `text not null` | |
| `body` | `text` | full text if available, nullable if headline-only feed |
| `author` | `text` | |
| `source_published_at` | `timestamptz not null` | |
| `source_fetched_at` | `timestamptz not null` | |
| `db_inserted_at` | `timestamptz not null default (now() at time zone 'utc')` | |
| `raw_payload` | `jsonb` | full raw response for reprocessing |
| `content_hash` | `text not null` | SHA-256 of `headline \|\| body`, used for dedup |
| `language` | `text` | ISO 639-1 |

**Indexes:**
- Unique on `(source_id, external_id)` where `external_id is not null`
- Unique on `(source_id, content_hash)` — primary dedup mechanism
- Btree on `source_published_at`
- Btree on `source_id, source_published_at`

### `article_entities`
Tagged entities per article. Populated at ingestion for Tier 1 sources (where entities are known from the source itself), left empty for Tier 2 at this stage.

| column | type | notes |
|---|---|---|
| `id` | `bigserial primary key` | |
| `article_id` | `bigint not null references articles(id) on delete cascade` | |
| `entity_type` | `text not null` | `ticker`, `person`, `org`, `event`, `release_type` |
| `entity_value` | `text not null` | e.g. `AAPL`, `Jerome Powell`, `FOMC`, `CPI` |
| `confidence` | `numeric` | nullable, for later NLP-tagged entities |

**Index:** `(entity_type, entity_value)` and `(article_id)`.

### `collection_runs`
Operational log of every collector invocation. Critical for detecting gaps.

| column | type | notes |
|---|---|---|
| `id` | `bigserial primary key` | |
| `source_id` | `int not null references sources(id)` | |
| `started_at` | `timestamptz not null` | |
| `finished_at` | `timestamptz` | null if still running / crashed |
| `status` | `text not null` | `success`, `partial`, `failed` |
| `articles_seen` | `int` | |
| `articles_inserted` | `int` | |
| `articles_duplicate` | `int` | |
| `error_message` | `text` | |

### Deduplication strategy
- Primary: `content_hash` (SHA-256 of normalized headline + body) unique per source.
- Secondary: `external_id` unique per source when the source provides one.
- **Cross-source duplicates are kept** — if Reuters and AP both publish the same story, we want both. Deduplication across sources happens at query time, not ingestion time. Rationale: source identity is itself signal.

### Schema migration
- Use Supabase SQL migrations, stored as versioned `.sql` files in the repo under `/migrations/`.
- Never alter existing columns destructively. Schema changes are additive only.

---

## Sources — Week-by-week build plan

### Week 1 — Architecture and foundation
**Deliverables:**
- GitHub repo initialized with project skeleton.
- DigitalOcean droplet provisioned, SSH configured, Python environment set up.
- Supabase schema created via migration files. Seed data for `sources` table loaded.
- `.env` protocol established. Secrets management documented.
- Logging, systemd service template, and healthcheck script patterns established.
- One end-to-end test: write a fake article row, read it back, confirm all three timestamps populate correctly.

**No real collectors yet.** The goal of Week 1 is that the next three weeks are pure "add a collector" work.

### Week 2 — Tier 1 collectors
Authoritative, timestamped, highest signal. All free.

1. **FOMC statements and minutes** — scraper for `federalreserve.gov/monetarypolicy/fomccalendars.htm` and statement/minutes pages. Runs every 4 hours.
2. **Fed speeches** — `federalreserve.gov/newsevents/speeches.htm` RSS if available, else scraper. Hourly.
3. **BLS economic releases** — CPI, NFP, PPI, employment situation. BLS has a schedule page and release archive. Hourly on release days, daily otherwise.
4. **Treasury releases** — `home.treasury.gov/news/press-releases` RSS. Hourly.
5. **SEC EDGAR filings** — filter to Mag 7 tickers (AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA) via EDGAR's full-text search or RSS by CIK. Every 15 minutes (EDGAR publishes rapidly).

**Each collector:**
- Runs as its own systemd service or cron job.
- Writes a `collection_runs` row on every invocation.
- Uses exponential backoff on failures.
- Never crashes the whole pipeline on one source failure.

### Week 3 — Tier 2 collectors
Broad coverage, less authoritative, higher volume.

1. **GDELT GKG (Global Knowledge Graph)** — daily bulk download from `data.gdeltproject.org`. Backfill from 2015 on initial run. Then daily incremental.
2. **Reuters RSS** — top news, business, markets feeds.
3. **AP RSS** — business and markets feeds.
4. **Major company press release wires** — Business Wire, PR Newswire, GlobeNewswire RSS, filtered to Mag 7 + macro-relevant tickers.
5. **Economic calendar** — ForexFactory or similar for scheduled release metadata (so we can tag articles to specific events later).

**GDELT note:** GDELT is big. Filter on ingestion to rows containing any of: Fed-related themes, macro themes (ECON_*, WB_*), Mag 7 tickers, or a curated keyword list. Store the filter used in `collection_runs.notes` so we can re-run with a broader filter later if needed.

### Week 4 — Reliability and monitoring
No new sources. Focus entirely on making the system trustworthy.

1. **Gap detection** — for each source, expected cadence is known. A daily script flags any source that hasn't produced a successful `collection_runs` row within its expected window.
2. **Daily integrity report** — emailed or posted to a Telegram/Discord webhook. Contents: articles collected per source in last 24h, failed runs, duplicate rates, disk usage, DB size.
3. **Healthcheck endpoint or heartbeat** — simple cron that hits healthchecks.io (free tier) every hour. If droplet dies, operator gets alerted.
4. **Backup strategy** — Supabase has point-in-time recovery, but also export a weekly snapshot of the `articles` table to a second location (e.g. DigitalOcean Spaces, ~$5/mo) as defense in depth.
5. **Runbook** — documented procedure for: adding a new source, dealing with a failing source, restoring from backup, rotating credentials, resizing the droplet.

---

## Operational conventions

### Git discipline
- `main` branch is what's deployed.
- Every change goes through a commit with a clear message.
- Claude Code commits after each logical chunk of work.
- Deployment is `git pull` on the droplet followed by a systemd reload for affected services.

### Secrets
- Never in git. Ever.
- `.env` on the droplet, `.env.example` in the repo with placeholder values.
- Supabase service role key stored on droplet only. Anon key not used by the pipeline (writes require service role).

### Error handling
- Every collector wraps its main loop in try/except that logs the error and continues.
- Fatal errors (e.g. DB unreachable for >10 minutes) trigger a Telegram/Discord alert.
- No silent failures. If a collector can't parse a record, log it and skip — don't crash.

### Rate limiting and politeness
- Every collector respects `robots.txt` and source-specified rate limits.
- Default: no more than 1 request per 2 seconds per source unless source docs permit faster.
- User-Agent string identifies the operator with a contact email (standard courtesy for scraping public data).

### Testing
- Unit tests for parsers (given this RSS snippet, do we extract the right timestamp?).
- Integration test: run each collector against live source, assert at least one row inserted in the last N minutes.
- Test suite runs locally before every deploy.

---

## Cost summary

| item | cost |
|---|---|
| DigitalOcean droplet (smallest) | ~$6/mo |
| Supabase | existing free tier initially, ~$25/mo if it scales up |
| DigitalOcean Spaces (backup) | ~$5/mo (optional, add in Week 4) |
| Healthchecks.io | free tier |
| Domain/alerts (Telegram/Discord bot) | free |
| **Total** | **~$6–36/month** |

If Supabase free tier gets exceeded (likely after several months of GDELT ingestion), upgrade or migrate bulk archive rows to Parquet on Spaces.

---

## Definition of done

The project is considered complete when:

1. All Tier 1 and Tier 2 sources are live and have been collecting without manual intervention for 14 consecutive days.
2. Daily integrity report is landing in the operator's inbox/chat.
3. Gap detection has successfully flagged at least one real gap (either induced for testing or natural).
4. A backtest-style query ("give me all FOMC statements between 2024-01-01 and 2024-06-30 with their exact publication timestamps") returns clean, correct data.
5. Runbook is written and the operator can walk through each procedure without help.
6. One full backup has been taken and one restore drill has been completed.

After that, the project enters maintenance mode: operator checks the daily report, adds new sources occasionally, and lets the archive compound.

---

## Post-completion roadmap (not in scope for this build)

Listed only so future-Claude and future-operator know what the archive is being built toward:

- NLP entity tagging backfill (once the archive has 3+ months of data).
- Sentiment scoring layer.
- Integration with backtesting harness when 7-year tick data is acquired.
- Regime-detection feature engineering using news flow rate, sentiment extremes, and event clustering.
- Tier 3 expansion (Twitter/X for Fed officials and financial journalists).

---

## Instructions for Claude Code

When working on this project:

1. **Read this spec first.** Re-read it at the start of each session. If a decision isn't covered here, ask the operator rather than guessing.
2. **Schema is sacred.** Never drop or destructively alter columns. Additive migrations only.
3. **Timestamps are sacred.** Every new collector must capture all three timestamp types. Code review yourself on this before committing.
4. **One source at a time.** Don't build three collectors in parallel. Ship one, verify it's running clean for 24h, then build the next.
5. **Operator is not a programmer.** Explain what you're doing in plain language in commit messages and PR descriptions. Avoid jargon when a status update is for the operator.
6. **Fail loudly, not silently.** Any data-quality risk (a parse that might be wrong, a timestamp that might be inferred rather than sourced) must be logged as a warning and surfaced in the daily report.
7. **Ask before spending.** Any change that would increase the monthly cost needs operator approval first.
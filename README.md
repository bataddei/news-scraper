# News Archive Pipeline

A 24/7 collector that archives financial news, Fed communications, economic releases, and SEC filings into Supabase Postgres — so a proprietary news history exists to backtest against when tick-level market data is acquired in 6–12 months.

**The archive is the product. The pipeline is infrastructure.** See [`project-brief.md`](./project-brief.md) for the full specification; that document is the source of truth.

---

## What this repo contains

```
.
├── project-brief.md          # Canonical specification — read this first
├── migrations/               # Versioned SQL migrations applied to Supabase
├── src/news_archive/         # Python package
│   ├── config.py             # Loads settings from .env
│   ├── db.py                 # Postgres connection helpers
│   ├── logging_config.py     # structlog setup
│   ├── models.py             # Pydantic models (Article, CollectionRun, ...)
│   ├── hashing.py            # content_hash for deduplication
│   ├── collectors/           # One module per source (Tier 1 in Week 2, Tier 2 in Week 3)
│   └── scripts/              # Operator-facing scripts (run_migrations, end_to_end_test, ...)
├── tests/                    # Unit tests
├── deploy/                   # systemd, cron, and logrotate templates for the droplet
├── docs/runbook.md           # Operator procedures (add source, handle failures, restore, ...)
└── .env.example              # Required env vars — copy to .env, never commit real .env
```

---

## Local setup (laptop)

### 1. Install Python 3.12

The pipeline targets Python 3.12 because that is what Ubuntu 24.04 ships on the droplet. Easiest path on macOS:

```bash
brew install python@3.12
```

### 2. Create a virtual environment and install

```bash
cd /Users/benitoalvareztaddei/code/news-scraper
python3.12 -m venv .venv
source .venv/bin/activate
make install-dev
```

### 3. Fill in `.env`

```bash
cp .env.example .env   # only if you don't already have one
```

Then edit `.env` and set, at minimum:

- `SUPABASE_DB_URL` — the Postgres connection string from Supabase → Project Settings → Database → Connection string → **Transaction pooler**. Replace `[YOUR-PASSWORD]` with your database password.
- `OPERATOR_CONTACT_EMAIL` — your email. Included in the User-Agent string when we scrape.

### 4. Apply migrations to your Supabase project

```bash
make migrate
```

This runs every `.sql` file in `migrations/` in version order against `SUPABASE_DB_URL`. Safe to re-run; each migration checks whether it's already applied.

### 5. Run the end-to-end test

```bash
make e2e
```

This inserts a fake article, reads it back, and verifies all three timestamps (`source_published_at`, `source_fetched_at`, `db_inserted_at`) are set correctly. If this passes, the foundation is good.

### 6. Run the unit tests

```bash
make test
```

---

## Deployment (droplet)

Deferred until the local build is proven. See [`docs/runbook.md`](./docs/runbook.md) once we get there.

---

## Design non-negotiables

These come straight from the brief and every change is reviewed against them:

1. **Every row stores three timestamps.** `source_published_at`, `source_fetched_at`, `db_inserted_at`. Backtest-available time = `GREATEST(source_published_at, source_fetched_at)`.
2. **Append-only, additive-only.** No destructive migrations. Ever.
3. **Cross-source duplicates are kept on purpose.** Source identity is signal. Dedup is within a source, by `content_hash` + `external_id`.
4. **Fail loudly, not silently.** Every uncertain parse logs a warning; every daily report surfaces them.
5. **One source at a time.** Ship, observe 24h clean, then the next.

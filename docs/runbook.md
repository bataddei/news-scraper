# Operator runbook

Procedures for running and maintaining the news archive. Written in plain language; each procedure is a checklist you can follow top-to-bottom.

---

## Contents

1. [Local development (laptop)](#local-development-laptop)
2. [First-time Supabase setup](#first-time-supabase-setup)
3. [Running the end-to-end test](#running-the-end-to-end-test)
4. [Adding a new source](#adding-a-new-source)
5. [Dealing with a failing source](#dealing-with-a-failing-source)
6. [Daily integrity report](#daily-integrity-report)
7. [Gap detection](#gap-detection)
8. [Healthcheck heartbeat](#healthcheck-heartbeat)
9. [Deploying a code change to the droplet](#deploying-a-code-change-to-the-droplet)
10. [Cleaning up GDELT article orphans](#cleaning-up-gdelt-article-orphans)
11. [Backup and restore drill](#backup-and-restore-drill)
12. [Rotating credentials](#rotating-credentials)
13. [Provisioning the DigitalOcean droplet](#provisioning-the-digitalocean-droplet)
14. [Resizing the droplet](#resizing-the-droplet)

---

## Local development (laptop)

Working directory: `/Users/benitoalvareztaddei/code/news-scraper`.

1. Make sure Python 3.13 is installed: `python3.13 --version`. If missing: `brew install python@3.13`.
2. Create a virtualenv and install the project with dev extras:
   ```bash
   python3.13 -m venv .venv
   source .venv/bin/activate
   make install-dev
   ```
3. Activate the venv in every new shell: `source .venv/bin/activate`.
4. Run the unit tests any time: `make test`.

---

## First-time Supabase setup

1. Log in to Supabase. Open the project you want to use for the archive.
2. **Project Settings → API** — copy `Project URL`, `anon public` key, and `service_role secret` into your local `.env` as `SUPABASE_URL`, `SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`.
3. **Project Settings → Database → Connection string** — pick the **Transaction pooler** tab. Copy the URI. Replace `[YOUR-PASSWORD]` with your database password. Paste into `.env` as `SUPABASE_DB_URL`.
4. Verify the variable name in `.env` matches `.env.example`.
5. Apply migrations:
   ```bash
   make migrate
   ```
   Expect output like `migrations.apply filename=0001_init_schema.sql` for each file, then `migrations.done`. Re-running is safe — it will say `migrations.up_to_date`.
6. In Supabase → Table editor, confirm the `news_archive` schema exists with tables `sources`, `articles`, `article_entities`, `collection_runs`, `schema_migrations`.

---

## Running the end-to-end test

```bash
make e2e
```

What it does: inserts one fake article, inserts a second copy (to prove dedup works), reads the article back, checks all three timestamps are populated and timezone-aware, then deletes the fake rows.

Expected: last log line says `e2e.pass`, and the script exits 0.

If it fails: the log lines above the failure will name the check that failed. Most common causes:
- `SUPABASE_DB_URL` missing/wrong → fix in `.env`.
- Migrations not applied → `make migrate`.
- `db_inserted_at out of expected window` → your laptop clock is drifting; fix system time.

---

## Adding a new source

1. Decide the source's `slug` (e.g. `reuters_commodities`), its tier (1 or 2), and its type (`rss` / `api` / `scraper` / `bulk`).
2. Add a new migration file under `/migrations/` with the next number (e.g. `0004_add_source_reuters_commodities.sql`). It should be a single `INSERT ... ON CONFLICT (slug) DO NOTHING` row. **Never edit a previous seed file.**
3. Run `make migrate` locally to apply the new seed row.
4. Add a collector module under `src/news_archive/collectors/` that subclasses `BaseCollector` and sets `source_slug`.
5. Run the collector once by hand to watch the log output and confirm rows land in `articles`.
6. Let it run for 24 hours on the droplet before you consider it production.
7. Add its schedule to `deploy/cron/news-pipeline.cron` or create a systemd instance for it.

---

## Dealing with a failing source

Symptom: the daily report shows failed runs for a source, or gap detection flags it as overdue / never-run.

### 1. Triage — what does the DB say?

In Supabase SQL editor:

```sql
select started_at, finished_at, status, articles_seen, articles_inserted, error_message
from news_archive.collection_runs
where source_id = (select id from news_archive.sources where slug = '<slug>')
order by started_at desc
limit 20;
```

Look at the pattern:
- **All failed, same error** → upstream change (feed format, URL moved, auth required) or network/DNS. Likely needs a code fix.
- **Flapping between success and failed** → transient upstream flakiness. Dedup absorbs it; leave it alone unless it keeps happening.
- **No rows at all** → the collector never ran. Check the cron/systemd layer (next step).

### 2. Check the scheduler on the droplet

```bash
ssh root@<droplet-ip>
# Cron logs (what cron actually fired and when)
journalctl -u cron.service --since "6 hours ago" | grep news_archive
# The collector's own logs (structured JSON from Python)
journalctl -t news-collector-<slug> --since "6 hours ago"
# Or for a cron-run collector, check the root journal for Python errors:
journalctl _COMM=python3.13 --since "2 hours ago" | grep -i <slug>
```

### 3. Run the collector by hand to reproduce

On the droplet, as the `news` user:

```bash
sudo -u news bash
cd /opt/news-scraper
.venv/bin/python -m news_archive.collectors.run <slug>
```

Read the last structured log line. Fields to look at: `status`, `articles_seen`, `error`. If it's an HTTP error, the response status and URL will be in the log.

### 4. Fix and redeploy

Never hotfix files on the droplet. Always: fix locally → commit → pull on droplet. See [Deploying a code change](#deploying-a-code-change-to-the-droplet).

### 5. Confirm recovery

After deploying, watch one successful run:

```bash
sudo -u news /opt/news-scraper/.venv/bin/python -m news_archive.collectors.run <slug>
# Then verify a fresh collection_runs row landed with status='success'.
make gap-check  # should no longer list this slug
```

---

## Daily integrity report

A Telegram message posted at **08:00 UTC every day** summarising the last 24 hours: articles inserted per source, failed runs, total archive size, droplet disk, and any active gaps.

### What the report includes

- **Per-source table**: `ins` (inserted), `dup` (duplicate), `seen` (total), `fail` (failed runs).
- **Archive stats**: total articles, Supabase DB size, droplet disk % used.
- **Gaps section**: anything `gap_check` currently flags (see below). ✅ if clean.

Only active collectors appear in the table. Dead seeded sources (Reuters, AP, Business Wire) are filtered out via `COLLECTORS` membership in `collectors/run.py`.

### Sending a report by hand

Useful for testing config changes or sending a one-off snapshot:

```bash
# Locally:
make daily-report

# On the droplet:
sudo -u news bash
cd /opt/news-scraper && .venv/bin/python -m news_archive.scripts.daily_report
```

Requires `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`.

### The report didn't arrive

1. Check the cron job actually fired at 08:00 UTC:
   ```bash
   journalctl -u cron.service --since today | grep daily_report
   ```
2. If it fired but the message never arrived:
   ```bash
   journalctl _COMM=python3.13 --since today | grep daily_report
   ```
   Look for `daily_report.send_failed` (Telegram API error) or `daily_report.missing_telegram_config` (env vars not loaded). A Telegram 401 usually means the bot token was rotated; a 400 with `chat not found` means the bot was removed from the chat.

### Changing the schedule

Edit the `daily_report` line in `deploy/cron/news-pipeline.cron` and reinstall:

```bash
sudo cp deploy/cron/news-pipeline.cron /etc/cron.d/news-pipeline
```

---

## Gap detection

"Which sources haven't successfully run in too long?" Per-source tolerance is ~2× the cron cadence (see `SOURCE_MAX_GAP_SECONDS` in `src/news_archive/monitoring/gaps.py`).

### Run a manual check

```bash
# Locally:
make gap-check

# On the droplet:
sudo -u news /opt/news-scraper/.venv/bin/python -m news_archive.scripts.gap_check
```

Exits 0 if clean, 1 if any gap is detected (output goes to stdout as structured logs).

Two gap kinds:
- **`never_run`** — source exists in `sources` but has zero rows in `collection_runs` with `status in ('success', 'partial')`. Usually means the collector was seeded but never wired up / never fired.
- **`overdue`** — last success is older than the tolerance. Either the collector is broken, or cron isn't firing it.

### Responding to a gap

1. Identify which slug is flagged. Follow [Dealing with a failing source](#dealing-with-a-failing-source) for that slug.
2. If the gap is expected (e.g. ForexFactory runs only twice a day and you checked between fires), no action — tolerance is already 14h.
3. After fixing, re-run `make gap-check` — it should exit 0.

### Tuning a tolerance

If a tolerance is too tight and pages on normal cadence, update `SOURCE_MAX_GAP_SECONDS` in `src/news_archive/monitoring/gaps.py`. Keep the inline cron-cadence comment in sync so the mapping stays readable.

---

## Healthcheck heartbeat

An hourly curl to healthchecks.io confirms the droplet is alive and cron is firing. If the droplet dies or cron stops, healthchecks.io notices the missed ping within ~2 hours and emails the operator.

### Setup

1. Create a check at healthchecks.io with period = 1 hour, grace = 1 hour.
2. Copy the ping URL into `/opt/news-scraper/.env` as `HEALTHCHECKS_URL=https://hc-ping.com/...`.
3. The cron line `0 * * * * news /opt/news-scraper/deploy/healthcheck.sh` is already installed; no further action.

### The heartbeat alarm fired

It means cron didn't run the healthcheck curl in the last hour. Possible causes, in order of likelihood:

1. **Droplet down / unreachable** — check the DigitalOcean console.
2. **Cron stopped** — `systemctl status cron` on the droplet.
3. **Network outage** — droplet is up but can't reach hc-ping.com. Log in and try `curl $HEALTHCHECKS_URL` manually.
4. **`.env` missing or unreadable by `news` user** — `healthcheck.sh` sources `.env`; if it can't, it exits without pinging. Check with `sudo -u news cat /opt/news-scraper/.env > /dev/null`.

### Temporarily pausing the check

While doing maintenance that might take the droplet down for more than an hour, pause the check in the healthchecks.io UI (don't delete — paused preserves history and the URL).

---

## Deploying a code change to the droplet

The droplet runs from a clone of the repo at `/opt/news-scraper`. Deployments are `git pull` + restart.

```bash
ssh root@<droplet-ip>
cd /opt/news-scraper
sudo -u news git pull
# If pyproject.toml / dependencies changed:
sudo -u news .venv/bin/pip install -e .
# If a migration was added:
sudo -u news .venv/bin/python -m news_archive.scripts.run_migrations
# If deploy/cron/news-pipeline.cron changed:
sudo cp deploy/cron/news-pipeline.cron /etc/cron.d/news-pipeline
# If a systemd unit changed:
sudo cp deploy/systemd/news-collector@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart 'news-collector@*.service'
```

Run `make gap-check` on the droplet after deploying to confirm nothing regressed.

### Rollback

If a deploy breaks a collector, `sudo -u news git log --oneline -10`, pick the last good SHA, `sudo -u news git checkout <sha>`, repeat the migration/cron/systemd steps above with the old files. Then fix forward on a fresh commit rather than leaving the droplet in detached HEAD.

---

## Cleaning up GDELT article orphans

Unlike every other source, GDELT writes to `news_archive.gdelt_rollup_15min`, not the `articles` table. If an older version of the collector ever runs against the live DB — e.g. the deploy gap between cleanup and `git pull`, or someone running a stale checkout by hand — it will insert per-article GDELT rows into `articles` that don't belong there. Left in place, those orphans bloat the table, distort source-row counts, and aren't visible to the rollup-driven premarket query.

### Detect

In Supabase SQL editor or `psql`:

```sql
select count(*), min(source_fetched_at), max(source_fetched_at)
from news_archive.articles
where source_id = (select id from news_archive.sources where slug = 'gdelt_gkg');
```

A non-zero count means there are orphans. The fetch-time range tells you which run(s) inserted them — useful for tracing how the stale code got run.

### Sweep

From your laptop (uses `SUPABASE_DB_URL` from `.env`, so it acts on the live DB):

```bash
python -m news_archive.scripts.backfill_gdelt_rollups --rollup --delete --vacuum
```

What each stage does:
1. **`--rollup`** — aggregates every orphan row into `gdelt_rollup_15min`. Bucket logic mirrors the live collector (FOMC / INFLATION / MAG7_<ticker> / etc.). `INSERT ... ON CONFLICT DO NOTHING`, so existing rollup windows are no-ops.
2. **`--delete`** — removes orphan article rows in batches of 5,000. `article_entities` cascade-deletes via the FK.
3. **`--vacuum`** — VACUUM FULL on `articles` + `article_entities` to reclaim the TOAST. Runs through the Supabase pooler with `autocommit=True`.

Idempotent — safe to run anytime. Exits in seconds when there's nothing to clean. Run after every deploy that touches GDELT-related code, just to be safe.

---

## Backup and restore drill

*Backup strategy is not yet chosen — operator decision pending between Supabase built-in PITR (free on paid tier, 7-day retention) vs. weekly `pg_dump` to DigitalOcean Spaces (~$5/mo, indefinite retention).*

Once chosen, this section will cover:

- How backups are produced and where they live.
- How to verify a backup is good (list latest, check size is nonzero and within ±50% of previous).
- A quarterly **restore drill**: take the latest backup, restore it into a throwaway Supabase project, run `make e2e` against it, then delete the throwaway project. The point is to confirm backups are actually restorable — untested backups don't count.

---

## Rotating credentials

### Rotating the Supabase database password

1. Supabase → Project Settings → Database → **Reset database password**.
2. Copy the new password. Update `SUPABASE_DB_URL` in your laptop `.env` and in `/opt/news-scraper/.env` on the droplet.
3. On the droplet: `sudo systemctl restart 'news-collector@*.service'` so every collector reconnects with the new password.
4. Watch `journalctl -f` for 2 minutes to confirm no auth errors.

### Rotating the service role key

1. Supabase → Project Settings → API → **Generate new service role secret**.
2. Update `SUPABASE_SERVICE_ROLE_KEY` in both `.env` files.
3. Restart services as above.

### Never commit a rotated key

`.env` is gitignored; verify with `git status` after editing that the file is not listed.

---

## Provisioning the DigitalOcean droplet

Full step-by-step is in [`docs/start-server.md`](start-server.md). That document is the canonical install procedure — this runbook only summarises.

- **Size**: smallest reasonable droplet (~$6/mo).
- **OS**: Debian 13 (Trixie) — ships Python 3.13 out of the box.
- **Region**: closest to the Supabase project.
- **User**: pipeline runs as unprivileged `news` user (matches systemd unit's `User=news`).
- **Repo**: cloned to `/opt/news-scraper`; `.env` copied out of band (never via git).
- **Scheduler**: cron file at `/etc/cron.d/news-pipeline`, installed from `deploy/cron/news-pipeline.cron`.
- **Logs**: journald (no rsyslog on Debian 13). Plain-text logs in `/var/log/news-pipeline/*.log` rotated by logrotate.

---

## Resizing the droplet

Supabase is the expensive bit, not the droplet. Resize only if the droplet actually runs out of memory or disk.

### When to resize

- **Memory**: `journalctl -k | grep -i oom` shows the kernel killing Python processes.
- **Disk**: daily report shows droplet disk >85% used for several consecutive days, and the offender is something you can't trim (journald is already capped, `/opt/news-scraper/.venv` is ~300MB, cloned repo is small).

### Procedure

1. In the DigitalOcean console: Droplet → Resize. Choose "CPU and RAM only" for a reversible resize (disk-included resizes are one-way).
2. Power off the droplet when prompted. The resize takes a few minutes.
3. Power back on. SSH in, run `make gap-check` — all collectors should resume on their own via cron. If any collector was running at shutdown, its `collection_runs` row may be stuck in `status='running'`; it'll be superseded by the next cron tick.

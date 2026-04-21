# Operator runbook

Procedures for running and maintaining the news archive. Written in plain language; each procedure is a checklist you can follow top-to-bottom.

Sections marked *(Week N)* are placeholders — filled in as we reach that week.

---

## Contents

1. [Local development (laptop)](#local-development-laptop)
2. [First-time Supabase setup](#first-time-supabase-setup)
3. [Running the end-to-end test](#running-the-end-to-end-test)
4. [Adding a new source](#adding-a-new-source)
5. [Dealing with a failing source](#dealing-with-a-failing-source) *(Week 2+)*
6. [Daily integrity report](#daily-integrity-report) *(Week 4)*
7. [Gap detection](#gap-detection) *(Week 4)*
8. [Backup and restore drill](#backup-and-restore-drill) *(Week 4)*
9. [Rotating credentials](#rotating-credentials)
10. [Provisioning the DigitalOcean droplet](#provisioning-the-digitalocean-droplet) *(deferred)*
11. [Resizing the droplet](#resizing-the-droplet) *(Week 4)*

---

## Local development (laptop)

Working directory: `/Users/benitoalvareztaddei/code/news-scraper`.

1. Make sure Python 3.12 is installed: `python3.12 --version`. If missing: `brew install python@3.12`.
2. Create a virtualenv and install the project with dev extras:
   ```bash
   python3.12 -m venv .venv
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

*(Populated in Week 2 when the first collectors exist.)*

General shape will be:
1. Check `collection_runs` for that `source_id` ordered by `started_at desc` — what's the latest `status` and `error_message`?
2. Check the systemd journal: `journalctl -u news-collector@<slug>.service -n 200`.
3. If the source changed its feed format, update the parser; don't hotfix prod — commit a fix and redeploy.

---

## Daily integrity report

*(Week 4.)*

---

## Gap detection

*(Week 4.)*

---

## Backup and restore drill

*(Week 4.)*

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

*(Deferred until the local build is proven.)*

High-level plan when we get there:
1. Create smallest reasonable droplet (~$6/mo, Ubuntu 24.04 LTS) in the region closest to the Supabase project.
2. Add operator's SSH key during creation.
3. SSH in, create a `news` user, install Python 3.12, clone the repo to `/opt/news-scraper`.
4. Copy `.env` over (out of band — never via git).
5. Install systemd template + cron file from `deploy/`.
6. Enable each collector one at a time; watch logs for 24h before enabling the next.

---

## Resizing the droplet

*(Week 4 or later.)*

Supabase is the expensive bit, not the droplet. Resize only if the droplet actually runs out of memory (check `journalctl -k | grep -i oom`).

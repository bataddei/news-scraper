# Droplet setup — from bare Debian to running pipeline

Each step runs as `root` unless noted. Copy/paste one block at a time and
check the output before moving on. If something looks off, stop and ask —
don't plow through.

---

## Step 1: Connect to server

SSH into your DigitalOcean droplet as root.

```bash
ssh -i ~/.ssh/id_tws root@162.243.161.40
```

---

## Step 2: Basic droplet hardening

Before installing anything, patch the OS and lock down SSH + firewall.

```bash
apt update && apt upgrade -y
apt install -y ufw fail2ban unattended-upgrades
```

Enable automatic security updates:

```bash
dpkg-reconfigure -plow unattended-upgrades
# Answer "Yes" to the prompt.
```

Firewall: allow SSH only, deny everything else inbound.

```bash
ufw allow OpenSSH
ufw --force enable
ufw status
```

Expected: `Status: active`, one rule allowing 22/tcp.

---

## Step 3: Create the `news` service user

The pipeline runs as an unprivileged user, not root. This matches the
systemd unit's `User=news` / `Group=news`.

```bash
adduser --system --group --home /opt/news-scraper --shell /bin/bash news
```

Verify:

```bash
id news
# Expected: uid=... gid=... groups=...
```

---

## Step 4: Install system packages

Our code requires Python >= 3.12. Which package to install depends on your
Debian version:

```bash
cat /etc/debian_version
```

### If Debian 13 (Trixie) — ships Python 3.13

```bash
apt install -y python3.13 python3.13-venv python3-pip git curl build-essential
```

Remember: wherever the rest of this guide says `python3.12`, use
`python3.13` instead. (3.13 satisfies our `>=3.12` requirement.)

### Verify

```bash
python3.13 --version   # or python3.12 on pyenv path
# Expected: Python 3.13.x (or 3.12.x)
```

---

## Step 5: Create log directory

The systemd unit grants `ReadWritePaths=/var/log/news-pipeline`, so this
directory must exist and be writable by `news`.

```bash
mkdir -p /var/log/news-pipeline
chown news:news /var/log/news-pipeline
chmod 750 /var/log/news-pipeline
```

---

## Step 6: Pull the repo onto the droplet

Two options. **Option A is recommended** (uses SSH, matches dev workflow).

### Option A: clone from GitHub

On your laptop, make sure the code is pushed:

```bash
# On your laptop
git push origin main
```

On the droplet, as `news`:

```bash
sudo -u news -H bash
cd /opt/news-scraper
git clone https://github.com/<your-user>/news-scraper.git .
exit
```

Replace `<your-user>` with your GitHub username. If the repo is private,
use an SSH deploy key or a personal access token URL instead.

### Option B: rsync from your laptop

From your **laptop** (not the droplet):

```bash
rsync -avz --exclude='.venv' --exclude='__pycache__' --exclude='.pytest_cache' \
    -e "ssh -i ~/.ssh/id_tws" \
    /Users/benitoalvareztaddei/code/news-scraper/ \
    root@162.243.161.40:/opt/news-scraper/

ssh -i ~/.ssh/id_tws root@162.243.161.40 'chown -R news:news /opt/news-scraper'
```

---

## Step 7: Set up the Python virtualenv

As `news`:

```bash
sudo -u news -H bash
cd /opt/news-scraper
python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install -e .
exit
```

The editable install (`pip install -e .`) is what makes
`python -m news_archive.collectors.run ...` work.

Smoke test — no DB hit yet, just verify the package imports:

```bash
sudo -u news /opt/news-scraper/.venv/bin/python -c "from news_archive.collectors.run import COLLECTORS; print(sorted(COLLECTORS))"
```

Expected: the list of 9 collector slugs.

---

## Step 8: Copy the `.env` file

The `.env` is **not** in git (it has secrets). Copy it from your laptop:

```bash
# On your laptop
scp -i ~/.ssh/id_tws /Users/benitoalvareztaddei/code/news-scraper/.env \
    root@162.243.161.40:/opt/news-scraper/.env
```

On the droplet:

```bash
chown news:news /opt/news-scraper/.env
chmod 600 /opt/news-scraper/.env
```

Edit it to set `ENVIRONMENT=droplet` (so rows collected here are tagged
distinctly from the laptop):

```bash
sed -i 's/^ENVIRONMENT=.*/ENVIRONMENT=droplet/' /opt/news-scraper/.env
grep ENVIRONMENT /opt/news-scraper/.env
# Expected: ENVIRONMENT=droplet
```

---

## Step 9: Verify DB connectivity

Run one cheap collector end-to-end:

```bash
sudo -u news bash -c 'cd /opt/news-scraper && .venv/bin/python -m news_archive.collectors.run fed_fomc_statements'
```

Expected: structured-JSON log lines, final line has `"status": "success"`.

If this fails with a connection error: `.env` is wrong or Supabase
blocks the droplet's IP. Stop and fix before continuing.

---

## Step 10: Install cron schedules

The cron file runs collectors as the `news` user at staggered minutes.

```bash
cp /opt/news-scraper/deploy/cron/news-pipeline.cron /etc/cron.d/news-pipeline
chmod 644 /etc/cron.d/news-pipeline
systemctl restart cron
```

Verify cron picked up the file:

```bash
systemctl status cron --no-pager
grep news-pipeline /var/log/syslog | tail -5
```

Expected: `Active: active (running)` and recent log lines mentioning
`(news-pipeline)`.

---

## Step 11: (Optional) Install systemd template for long-lived collectors

Only needed if you later add a collector that should run continuously
rather than on a cron cadence. Not required for the current pipeline —
all 9 collectors are cron-driven.

```bash
cp /opt/news-scraper/deploy/systemd/news-collector@.service /etc/systemd/system/
systemctl daemon-reload
# Example: enable a continuous collector:
# systemctl enable --now news-collector@<slug>.service
```

---

## Step 12: Install logrotate

```bash
cp /opt/news-scraper/deploy/logrotate.conf /etc/logrotate.d/news-pipeline
logrotate -d /etc/logrotate.d/news-pipeline
# "-d" runs in debug/dry mode — check for errors.
```

---

## Step 13: First real verification

Wait ~15 minutes for the SEC EDGAR collector (`*/15 * * * *`) to fire,
then check:

```bash
# Cron saw the job fire
grep CRON /var/log/syslog | grep news-pipeline | tail -10

# Collector logged a successful run
sudo -u news bash -c 'cd /opt/news-scraper && .venv/bin/python -c "
from news_archive import db
with db.pool().connection() as conn, conn.cursor() as cur:
    cur.execute(\"SELECT source_id, status, articles_inserted, finished_at FROM news_archive.collection_runs ORDER BY finished_at DESC NULLS LAST LIMIT 10\")
    for row in cur.fetchall(): print(row)
"'
```

Expected: recent rows with `status='success'` and `environment='droplet'`
visible when you inspect rows directly.

---

## Step 14: Quick reference

| Action | Command |
|---|---|
| Tail all collector logs (journald) | `journalctl -u 'news-collector@*' -f` |
| Tail cron fires | `grep news-pipeline /var/log/syslog` |
| Run one collector by hand | `sudo -u news bash -c 'cd /opt/news-scraper && .venv/bin/python -m news_archive.collectors.run <slug>'` |
| Pull latest code | `sudo -u news bash -c 'cd /opt/news-scraper && git pull'` |
| Reinstall deps after pull | `sudo -u news bash -c 'cd /opt/news-scraper && .venv/bin/pip install -r requirements.txt && .venv/bin/pip install -e .'` |

---

## Step 15: What's deliberately NOT done yet

- Healthchecks.io hourly ping — `HEALTHCHECKS_URL` is blank in `.env`.
  Week 4 task.
- Telegram/Discord alerts on run failures — also Week 4.
- Supabase DB backup cron — Week 4.
- Daily integrity report — Week 4.

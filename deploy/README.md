# Deploy artifacts

Templates for the DigitalOcean droplet. **Nothing in this folder is installed locally.** Week 1 keeps these as files only; they're applied on the droplet once the pipeline is proven on the laptop.

| file | purpose |
|---|---|
| `systemd/news-collector@.service` | Template systemd unit — one instance per source slug |
| `cron/news-pipeline.cron` | Cadenced collectors and daily report schedule |
| `healthcheck.sh` | Hourly curl to healthchecks.io so we notice if the droplet dies |
| `logrotate.conf` | Plain-text log rotation (journald handles its own rotation) |

See `docs/runbook.md` for the full install procedure on the droplet.

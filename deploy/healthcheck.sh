#!/usr/bin/env bash
# Hourly heartbeat to healthchecks.io. If the droplet dies, healthchecks.io
# notices the missed ping and alerts the operator.
#
# Requires HEALTHCHECKS_URL in /opt/news-scraper/.env.

set -euo pipefail

ENV_FILE="/opt/news-scraper/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "healthcheck: env file not found at ${ENV_FILE}" >&2
    exit 1
fi

# shellcheck disable=SC1090
source "${ENV_FILE}"

if [[ -z "${HEALTHCHECKS_URL:-}" ]]; then
    # Not configured yet — exit quietly. Week 4 will set this up.
    exit 0
fi

curl -fsS --max-time 10 --retry 3 "${HEALTHCHECKS_URL}" >/dev/null

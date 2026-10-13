#!/usr/bin/env bash
# Build local tag cainvest/hummingbot:latest (not on Docker Hub).
# Use from cron if nightly prune removes unused images, e.g.:
#   15 5 * * * /path/to/hummingbot/hummingbot/build-cainvest-image.sh >>/var/log/cainvest-hb-image.log 2>&1
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
exec docker build -t cainvest/hummingbot:latest "$ROOT"

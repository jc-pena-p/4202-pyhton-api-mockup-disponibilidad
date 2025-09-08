#!/usr/bin/env bash
set -euo pipefail

INTERVAL="${INTERVAL_SECONDS:-5}"   # intervalo en segundos

echo "monitor runner: interval ${INTERVAL}s" >&2
date -u +"%Y-%m-%dT%H:%M:%SZ monitor runner started (every ${INTERVAL}s)" >> /var/log/consulta/monitor.log

while true; do
  /app/monitor.sh || true
  sleep "$INTERVAL"
done

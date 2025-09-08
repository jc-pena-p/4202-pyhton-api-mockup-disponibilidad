#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${OUT_DIR:-/var/log/consulta}"
STATE_FILE="${STATE_FILE:-/var/cache/consulta/monitor_state.json}"

UPSTREAM_HEALTH_URL="${UPSTREAM_HEALTH_URL:-https://maestria.codezor.dev/health}"

FAIL_THRESHOLD="${FAIL_THRESHOLD:-3}"  
FAIL_WINDOW="${FAIL_WINDOW:-5}"       

mkdir -p "$OUT_DIR" "$(dirname "$STATE_FILE")" "$OUT_DIR/incidents"

now_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
printf '{"ts":"%s","event":"tick"}\n' "$now_iso" >> "$OUT_DIR/monitor.log"

# --- Cargar estado previo ---
if [[ -f "$STATE_FILE" ]]; then
  state="$(cat "$STATE_FILE")"
else
  state='{}'
fi

check_http() {
  local url="$1" timeout="${2:-2}"
  local t0 t1 http_code dur
  t0=$(date +%s%3N)
  http_code=$(curl -sS -o /dev/null -m "$timeout" -w "%{http_code}" "$url" || echo "000")
  t1=$(date +%s%3N)
  dur=$((t1 - t0))
  printf "%s %s\n" "$http_code" "$dur"
}

read -r code dur_ms < <(check_http "$UPSTREAM_HEALTH_URL" 2)

prev_fail="$(echo "$state" | jq -r '.upstream.consecutive_failures // 0' 2>/dev/null || echo 0)"

if [[ "$code" == 2* ]]; then
  consec_fail=0
else
  consec_fail=$((prev_fail + 1))
fi

state="$(echo "$state" | jq \
  --arg now "$now_iso" \
  --arg code "$code" \
  --argjson dur "$dur_ms" \
  --arg url "$UPSTREAM_HEALTH_URL" \
  --argjson win "$FAIL_WINDOW" \
  --argjson cf "$consec_fail" '
  .upstream //= {history: [], consecutive_failures: 0} |
  .upstream.history += [{ts:$now, code:$code, duration_ms:$dur, url:$url}] |
  .upstream.history |= (.[-($win):] // .) |
  .upstream.consecutive_failures = $cf
')"

already_open="$(echo "$state" | jq -r '.upstream.open_incident // "no"')"

if (( consec_fail >= FAIL_THRESHOLD )); then
  if [[ "$already_open" != "yes" ]]; then
    incident_file="$OUT_DIR/incidents/incident-upstream-$(date -u +%Y%m%dT%H%M%SZ).json"
    echo "$state" | jq \
      --arg code "$code" \
      --arg url "$UPSTREAM_HEALTH_URL" \
      --argjson consec "$consec_fail" \
      '{type:"upstream_health", status:"open", first_seen:(now|todate),
        url:$url, consecutive_failures:$consec, last_code:$code,
        history:.upstream.history}' > "$incident_file"
    echo "opened incident: $incident_file" >> "$OUT_DIR/monitor.log"
    state="$(echo "$state" | jq '.upstream.open_incident="yes"')"
  fi
else
  if [[ "$already_open" == "yes" ]]; then
    incident_file="$OUT_DIR/incidents/incident-upstream-$(date -u +%Y%m%dT%H%M%SZ)-resolved.json"
    echo "$state" | jq \
      '{type:"upstream_health", status:"resolved",
        resolved_at:(now|todate), history:.upstream.history}' > "$incident_file"
    echo "resolved incident: $incident_file" >> "$OUT_DIR/monitor.log"
    state="$(echo "$state" | jq 'del(.upstream.open_incident)')"
  fi
fi

echo "$state" > "$STATE_FILE"
echo "{\"ts\":\"$now_iso\",\"check\":\"upstream\",\"url\":\"$UPSTREAM_HEALTH_URL\",\"http_code\":\"$code\",\"duration_ms\":$dur_ms}" \
  >> "$OUT_DIR/monitor.jsonl"

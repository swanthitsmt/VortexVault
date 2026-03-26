#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <bucket> <object_key_2gb> [object_key_20gb] [base_url]" >&2
  exit 1
fi

BUCKET="$1"
OBJECT_2GB="$2"
OBJECT_20GB="${3:-}"
BASE_URL="${4:-http://localhost:8000}"

start_ingest() {
  local bucket="$1"
  local object_key="$2"
  curl -sS -X POST "${BASE_URL}/api/v2/ingest/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"source_bucket\":\"${bucket}\",\"source_object\":\"${object_key}\",\"auto_merge\":true}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

poll_ingest() {
  local job_id="$1"
  for i in $(seq 1 7200); do
    payload=$(curl -sS "${BASE_URL}/api/v2/ingest/jobs/${job_id}")
    status=$(printf '%s' "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
    processed=$(printf '%s' "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["processed_lines"])')
    indexed=$(printf '%s' "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["indexed_docs"])')
    echo "[$i] status=${status} processed=${processed} indexed=${indexed}"
    if [ "$status" = "completed" ] || [ "$status" = "failed" ]; then
      break
    fi
    sleep 2
  done
}

run_case() {
  local object_key="$1"
  echo "=== ingest benchmark: ${object_key} ==="
  local job_id
  job_id="$(start_ingest "$BUCKET" "$object_key")"
  echo "job_id=${job_id}"
  poll_ingest "$job_id"
}

run_case "$OBJECT_2GB"
if [ -n "$OBJECT_20GB" ]; then
  run_case "$OBJECT_20GB"
fi


echo "Benchmark matrix completed."

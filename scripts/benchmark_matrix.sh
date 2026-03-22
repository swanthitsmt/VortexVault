#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <auth_user> <auth_pass> <input_path_2gb> [input_path_20gb]" >&2
  exit 1
fi

AUTH_USER="$1"
AUTH_PASS="$2"
INPUT_2GB="$3"
INPUT_20GB="${4:-}"
BASE_URL="${BASE_URL:-http://localhost:18000}"

run_pipeline() {
  local src="$1"
  echo "=== Pipeline run for: $src ==="
  local resp
  resp=$(curl -k -sS -u "$AUTH_USER:$AUTH_PASS" \
    -H 'Content-Type: application/json' \
    -d "{\"source_path\":\"$src\"}" \
    "$BASE_URL/api/jobs/pipeline")

  local clean_id merge_id
  clean_id=$(printf '%s' "$resp" | python3 -c 'import json,sys; o=json.load(sys.stdin); print(o["clean_job"]["id"])')
  merge_id=$(printf '%s' "$resp" | python3 -c 'import json,sys; o=json.load(sys.stdin); print(o["merge_job"]["id"])')

  echo "clean_job=$clean_id"
  echo "merge_job=$merge_id"

  for i in $(seq 1 3600); do
    status=$(curl -k -sS -u "$AUTH_USER:$AUTH_PASS" "$BASE_URL/api/jobs/$merge_id" | python3 -c 'import json,sys; o=json.load(sys.stdin); print(o.get("status"))')
    echo "[$i] merge_status=$status"
    if [ "$status" = "completed" ] || [ "$status" = "failed" ] || [ "$status" = "paused" ]; then
      break
    fi
    sleep 2
  done
}

run_pipeline "$INPUT_2GB"
if [ -n "$INPUT_20GB" ]; then
  run_pipeline "$INPUT_20GB"
fi

echo "Benchmark matrix run completed."

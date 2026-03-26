#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <query> <line_limit> <output_parquet> [base_url]" >&2
  exit 1
fi

QUERY="$1"
LINE_LIMIT="$2"
OUT_FILE="$3"
BASE_URL="${4:-http://localhost:8000}"

job_id=$(curl -sS -X POST "${BASE_URL}/api/v2/exports" \
  -H 'Content-Type: application/json' \
  -d "{\"query\":\"${QUERY}\",\"line_limit\":${LINE_LIMIT}}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')

echo "export_job_id=${job_id}"

for i in $(seq 1 1800); do
  payload=$(curl -sS "${BASE_URL}/api/v2/exports/${job_id}")
  status=$(printf '%s' "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  rows=$(printf '%s' "$payload" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("exported_rows", 0))')
  echo "[$i] status=${status} exported_rows=${rows}"
  if [ "$status" = "completed" ]; then
    break
  fi
  if [ "$status" = "failed" ]; then
    echo "Export failed" >&2
    exit 1
  fi
  sleep 2
done

download_url=$(curl -sS "${BASE_URL}/api/v2/exports/${job_id}/download" | python3 -c 'import json,sys; print(json.load(sys.stdin)["download_url"])')
curl -sS "$download_url" -o "$OUT_FILE"

echo "parquet_file=${OUT_FILE}"
if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY' "$OUT_FILE"
import pyarrow.parquet as pq
import sys

table = pq.read_table(sys.argv[1])
print(f"rows={table.num_rows}")
print(f"columns={table.num_columns}")
PY
fi

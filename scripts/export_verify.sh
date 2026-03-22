#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <auth_user> <auth_pass> <url_filter> <output_csv> [base_url]" >&2
  exit 1
fi

AUTH_USER="$1"
AUTH_PASS="$2"
URL_FILTER="$3"
OUT_FILE="$4"
BASE_URL="${5:-http://localhost:18000}"

curl -sS -u "$AUTH_USER:$AUTH_PASS" \
  "$BASE_URL/search/export?url=$URL_FILTER" \
  -o "$OUT_FILE"

TOTAL_LINES=$(wc -l < "$OUT_FILE" | tr -d ' ')
DATA_LINES=$((TOTAL_LINES > 0 ? TOTAL_LINES - 1 : 0))

echo "csv_file=$OUT_FILE"
echo "total_lines=$TOTAL_LINES"
echo "data_lines=$DATA_LINES"

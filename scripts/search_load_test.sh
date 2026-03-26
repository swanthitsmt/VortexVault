#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "Usage: $0 <query> <requests> <concurrency> [base_url]" >&2
  exit 1
fi

QUERY="$1"
REQUESTS="$2"
CONCURRENCY="$3"
BASE_URL="${4:-http://localhost:8000}"
TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

run_one() {
  auth_args=()
  if [ -n "${API_AUTH_TOKEN:-}" ]; then
    auth_args=(-H "Authorization: Bearer ${API_AUTH_TOKEN}")
  fi
  curl -sS -o /dev/null -w "%{time_total}\n" \
    -X POST "${BASE_URL}/api/v2/search/query" \
    "${auth_args[@]}" \
    -H 'Content-Type: application/json' \
    -d "{\"query\":\"${QUERY}\",\"limit\":50,\"prefix\":true,\"typo_tolerance\":true}" || echo "ERR"
}

export -f run_one
export BASE_URL QUERY API_AUTH_TOKEN

seq "$REQUESTS" | xargs -I{} -P "$CONCURRENCY" bash -lc 'run_one' >> "$TMP_FILE"

python3 - <<'PY' "$TMP_FILE"
import sys
from statistics import median

vals = []
errs = 0
for line in open(sys.argv[1], encoding="utf-8"):
    line = line.strip()
    if not line or line == "ERR":
        errs += 1
        continue
    vals.append(float(line))

if not vals:
    print("no_successful_requests")
    sys.exit(1)

vals.sort()
p95_idx = max(int(len(vals) * 0.95) - 1, 0)
print(f"requests_ok={len(vals)} requests_err={errs}")
print(f"p50_sec={median(vals):.4f}")
print(f"p95_sec={vals[p95_idx]:.4f}")
print(f"max_sec={vals[-1]:.4f}")
PY

#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 5 ]; then
  echo "Usage: $0 <auth_user> <auth_pass> <url_filter> <requests> <concurrency> [base_url]" >&2
  exit 1
fi

AUTH_USER="$1"
AUTH_PASS="$2"
URL_FILTER="$3"
REQUESTS="$4"
CONCURRENCY="$5"
BASE_URL="${6:-http://localhost:18000}"

TMP_FILE="$(mktemp)"
trap 'rm -f "$TMP_FILE"' EXIT

run_one() {
  curl -sS -u "$AUTH_USER:$AUTH_PASS" \
    -o /dev/null \
    -w "%{time_total}\n" \
    "$BASE_URL/search?url=$URL_FILTER&page_size=25" || echo "ERR"
}

export -f run_one
export AUTH_USER AUTH_PASS URL_FILTER BASE_URL

seq "$REQUESTS" | xargs -I{} -P "$CONCURRENCY" bash -lc 'run_one' >> "$TMP_FILE"

python3 - <<'PY' "$TMP_FILE"
import sys
from statistics import median

vals = []
errs = 0
for line in open(sys.argv[1]):
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

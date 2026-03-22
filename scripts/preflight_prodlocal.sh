#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ ! -f "infra/nginx/certs/tls.crt" ] || [ ! -f "infra/nginx/certs/tls.key" ]; then
  echo "TLS certs not found, generating self-signed certs..."
  ./scripts/generate_local_tls_cert.sh
fi

cp .env.prodlocal .env
set -a
source .env
set +a

AUTH_USER="${BASIC_AUTH_USERNAME:-admin}"
AUTH_PASS="${BASIC_AUTH_PASSWORD:-admin123}"

docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build

echo "Waiting for core services..."
sleep 5
docker compose ps

echo "Checking dashboard endpoint..."
if ! curl -sf -u "$AUTH_USER:$AUTH_PASS" http://localhost:18000/dashboard >/dev/null; then
  echo "Dashboard check failed" >&2
  exit 1
fi

echo "Prodlocal preflight baseline is up."

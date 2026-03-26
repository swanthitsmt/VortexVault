#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cp .env.prodlocal .env

docker compose -f docker-compose.yml -f docker-compose.prodlocal.yml up -d --build

echo "Waiting for services..."
for _ in $(seq 1 60); do
  if curl -sf "http://localhost:${EDGE_BIND_PORT:-8000}/health" >/dev/null; then
    break
  fi
  sleep 2
done

curl -sf "http://localhost:${EDGE_BIND_PORT:-8000}/health" >/dev/null
curl -sf "http://localhost:${EDGE_BIND_PORT:-8000}/api/v2/dashboard" >/dev/null

docker compose ps

echo "Prodlocal preflight passed."

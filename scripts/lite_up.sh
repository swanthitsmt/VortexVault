#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

cp .env.lite .env

docker compose -f docker-compose.lite.yml up -d --build

echo "VortexVault Lite is starting..."
echo "UI: http://localhost:8000"
echo "API docs: http://localhost:8000/docs"
echo "Flower: http://localhost:5556"
echo "MinIO Console: http://localhost:9002"

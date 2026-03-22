#!/usr/bin/env bash
set -euo pipefail

if [ ! -f .env ]; then
  cp .env.example .env
fi

docker compose up -d --build

echo "App: http://localhost:8000"
echo "Flower: http://localhost:5555"

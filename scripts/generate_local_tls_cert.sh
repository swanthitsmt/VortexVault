#!/usr/bin/env bash
set -euo pipefail

CERT_DIR="$(cd "$(dirname "$0")/.." && pwd)/infra/nginx/certs"
mkdir -p "$CERT_DIR"

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl not found" >&2
  exit 1
fi

openssl req -x509 -nodes -newkey rsa:4096 \
  -keyout "$CERT_DIR/tls.key" \
  -out "$CERT_DIR/tls.crt" \
  -days 825 \
  -subj "/CN=vortexvault.local"

echo "Generated: $CERT_DIR/tls.crt"
echo "Generated: $CERT_DIR/tls.key"

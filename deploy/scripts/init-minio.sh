#!/bin/sh
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "local/${MINIO_BUCKET_RAW}"
mc mb --ignore-existing "local/${MINIO_BUCKET_EXPORT}"
mc anonymous set private "local/${MINIO_BUCKET_RAW}"
mc anonymous set private "local/${MINIO_BUCKET_EXPORT}"

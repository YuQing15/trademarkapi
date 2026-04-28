#!/usr/bin/env bash
set -euo pipefail

DB_DIR="data"
DB_PATH="${DB_DIR}/trademarks_clean.sqlite"
DB_URL="https://github.com/YuQing15/trademarkapi/releases/download/v1.0.9/trademarks_clean.sqlite"

mkdir -p "${DB_DIR}"

if [ -f "${DB_PATH}" ]; then
  echo "Database already exists: ${DB_PATH}"
  exit 0
fi

echo "Downloading database to ${DB_PATH}"
curl -fL "${DB_URL}" -o "${DB_PATH}"
echo "Database download complete: ${DB_PATH}"

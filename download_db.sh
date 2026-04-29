#!/usr/bin/env bash
set -euo pipefail

DB_DIR="data"
DB_PATH="${DB_DIR}/trademarks_clean.sqlite"

PART_AA_URL="https://github.com/YuQing15/trademarkapi/releases/download/v1.1.0/trademarks_clean.sqlite.part_aa"
PART_AB_URL="https://github.com/YuQing15/trademarkapi/releases/download/v1.1.0/trademarks_clean.sqlite.part_ab"

PART_AA="${DB_DIR}/trademarks_clean.sqlite.part_aa"
PART_AB="${DB_DIR}/trademarks_clean.sqlite.part_ab"

mkdir -p "${DB_DIR}"

if [ -f "${DB_PATH}" ]; then
  echo "Database already exists: ${DB_PATH}"
  exit 0
fi

echo "Downloading database parts..."

curl -fL "$PART_AA_URL" -o "$PART_AA"
curl -fL "$PART_AB_URL" -o "$PART_AB"

echo "Merging parts..."

cat "$PART_AA" "$PART_AB" > "$DB_PATH"

echo "Cleaning up parts..."
rm "$PART_AA" "$PART_AB"

echo "Database ready: $DB_PATH"
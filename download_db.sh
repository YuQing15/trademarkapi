#!/usr/bin/env bash
set -euo pipefail

DB_DIR="data"
PARTS_DIR="${DB_DIR}/db_parts"
DB_PATH="${DB_DIR}/trademarks_clean.sqlite"
BASE_URL="https://github.com/YuQing15/trademarkapi/releases/download/v1.1.0"

mkdir -p "${DB_DIR}" "${PARTS_DIR}"

if [ -f "${DB_PATH}" ]; then
  echo "Database already exists"
  exit 0
fi

echo "Downloading split database parts into ${PARTS_DIR}"
curl -fL "${BASE_URL}/trademarks_clean.sqlite.part_aa" -o "${PARTS_DIR}/trademarks_clean.sqlite.part_aa"
curl -fL "${BASE_URL}/trademarks_clean.sqlite.part_ab" -o "${PARTS_DIR}/trademarks_clean.sqlite.part_ab"

echo "Combining parts into ${DB_PATH}"
cat "${PARTS_DIR}"/trademarks_clean.sqlite.part_* > "${DB_PATH}"

echo "Validating database"
if [ "$(sqlite3 "${DB_PATH}" "PRAGMA integrity_check;")" != "ok" ]; then
  echo "Database integrity check failed" >&2
  exit 1
fi

rm -rf "${PARTS_DIR}"
echo "Database download and reconstruction complete: ${DB_PATH}"

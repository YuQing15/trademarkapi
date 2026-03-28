#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

DB_PATH="data/trademarks.sqlite"

echo "Rebuilding trademark index..."
python3 scripts/build_index.py

if [[ ! -f "$DB_PATH" ]]; then
  echo "Error: expected database not found at $DB_PATH" >&2
  exit 1
fi

echo
echo "Build complete."
echo "Database: $DB_PATH"
ls -lh "$DB_PATH"

if command -v shasum >/dev/null 2>&1; then
  echo
  echo "SHA256:"
  shasum -a 256 "$DB_PATH"
fi

echo
echo "Next steps:"
echo "1. Upload $DB_PATH to a new GitHub Release asset."
echo "2. Copy the direct download URL."
echo "3. Update TRADEMARK_DB_URL in Render."
echo "4. Trigger Manual Deploy in Render."

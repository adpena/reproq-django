#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv and re-run this script."
  exit 1
fi

uv sync --locked --extra dev --python "$(cat .python-version)"
uv run --locked pytest

echo "Done. Run tests with: uv run pytest"

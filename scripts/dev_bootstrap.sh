#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required. Install uv and re-run this script."
  exit 1
fi

uv venv .venv --python 3.12
source .venv/bin/activate
uv pip install -e ".[dev]"
uv run pytest

echo "Done. Run tests with: uv run pytest"

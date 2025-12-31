# Repository Guidelines

## Project Layout
- `src/reproq_django/`: core Django package (models, management commands, admin UI).
- `tests/`: pytest suite for integration and regression coverage.
- `scripts/`: local bootstrap helpers (install deps, run quick checks).
- `docs/`: design and usage documentation.

## Development Workflow
- Use `uv` for all Python commands to keep the locked environment consistent.
- Typical setup: `uv sync` then `uv run python manage.py reproq init`.
- Run management commands via `uv run python manage.py reproq <subcommand>`.

## Testing
- Always execute tests with `uv run pytest`.
- For focused runs: `uv run pytest tests/test_file.py -k pattern`.
- If you change models, run migrations locally and ensure related tests pass.

## Conventions
- Keep schema and SQL compatibility with the Go worker (`reproq-worker/internal/queue/queue.go`).
- Favor backwards-compatible changes for migrations and task schema.
- Keep exactly one migration file in `src/reproq_django/migrations/` unless absolutely necessary.
- Update version strings together (`pyproject.toml` and `src/reproq_django/__init__.py`) when releasing.
- Retain only three tags/releases at any time; delete older tags/releases and their artifacts.

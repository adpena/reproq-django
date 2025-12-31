# Reproq Django Agent Guidelines

As an agent working on `reproq-django`, you must adhere to the following standards:

## 1. Integration Integrity
- Ensure the Django schema and logic remain compatible with the Go `reproq-worker`.
- Use `uv` for all Python commands (install, migrations, tests).
- Run `uv run pytest` to verify changes in the locked environment.

## 2. Observability & API
- Enhance `reproq_stats_api` to provide high-level insights (e.g., periodic task status, worker health).
- Ensure the TUI metrics proxy is secure and performant.

## 3. Worker Lifecycle
- The `python manage.py reproq` command is the source of truth for worker management.
- Always verify that the Go binary installation/build process is robust across platforms.

## 4. Documentation & Release
- Keep `GEMINI.md` updated with any changes to the task schema or executor logic.
- Adhere to the migration policy: keep exactly one migration file in `src/reproq_django/migrations/` unless absolutely necessary.
- Update version strings together (`pyproject.toml` and `src/reproq_django/__init__.py`) when releasing.
- Retain only three tags/releases at any time; delete older tags/releases and their artifacts.

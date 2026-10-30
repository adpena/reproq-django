# Reproq Django Agent Guidelines

As an agent working on `reproq-django`, you must adhere to the following standards:

## 1. Integration Integrity
- Ensure the Django schema and logic remain compatible with the Go `reproq-worker`.
- Use `uv` for all Python commands (install, migrations, tests).
- Run `uv run pytest` to verify changes in the locked environment.
- Before landing Python compatibility or dependency changes, run both locked local checks on Python 3.12, 3.13, 3.14, and 3.15: `uv run --isolated --locked --extra dev --python <version> ruff check .` and `uv run --isolated --locked --extra dev --python <version> pytest`. Keep each interpreter's environment isolated and verify the installed package versions; a classifier alone is not compatibility evidence.

## 2. Observability & API
- Enhance `reproq_stats_api` to provide high-level insights (e.g., periodic task status, worker health).
- Ensure the TUI metrics proxy is secure and performant.

## 3. Worker Lifecycle
- The `python manage.py reproq` command is the source of truth for worker management.
- Always verify that the Go binary installation/build process is robust across platforms.

## 4. Documentation & Release
- Keep `GEMINI.md` updated with any changes to the task schema or executor logic.
- Preserve published migration bytes and append necessary forward migrations.
  Committed Django migrations are the sole queue-schema DDL authority; never
  add an independent SQL initializer or automatic table repair. Historical
  schema adoption must verify a supported full catalog and migration history
  before an explicit audited reconciliation. This necessary forward migration
  policy supersedes the former one-file preference.
- Update version strings together (`pyproject.toml` and `src/reproq_django/__init__.py`) when releasing.
- Preserve published tags, releases, and assets for reproducibility. Retire obsolete current code and active artifacts instead; never move a released tag or replace published assets.

## 5. Render Env Var Safety
- The Render API `PUT /services/{id}/env-vars` **replaces the entire env var set**.
- Always `GET` the current env vars first, compare with `render.yaml`, and only then apply changes.
- Never assume a partial update; verify you are not deleting required keys (e.g., `PYTHON_VERSION`, `DATABASE_URL`).

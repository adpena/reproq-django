# Deploying Reproq to Render

This guide explains how to configure your Render environment to run Reproq Worker and Beat processes alongside your Django web application.

## 1. Build Configuration
The Reproq Go worker binary is not bundled in the repository. You must download it during the Render build phase.

Update your `render.yaml` (or the **Build Command** in the Render Dashboard):

```bash
# Add this line to your build command
python manage.py reproq install
```

Example `render.yaml` build command:
```yaml
buildCommand: |
  uv sync --upgrade
  uv run python manage.py reproq install  # <--- Fetch the Go binary
  uv run python manage.py collectstatic --noinput
```

## 2. Worker Service
Background tasks require a separate service on Render. We recommend adding a **Background Worker** service.

Add this to your `render.yaml`:

```yaml
  - type: worker
    name: reproq-worker
    runtime: python
    buildCommand: uv sync --upgrade && uv run python manage.py reproq install
    startCommand: uv run python manage.py reproq worker
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: your-db-name
          property: connectionString
```

## 3. Beat Service (Optional)
If you use periodic tasks, you need exactly one instance of the `beat` process. You can run this as another background worker:

```yaml
  - type: worker
    name: reproq-beat
    runtime: python
    buildCommand: uv sync --upgrade && uv run python manage.py reproq install
    startCommand: uv run python manage.py reproq beat
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: your-db-name
          property: connectionString
```

## 4. Environment Variables
Ensure the following variables are set in your Render environment:
- `DATABASE_URL`: Required for the Go worker to connect to PostgreSQL.
- `REPROQ_WORKER_BIN`: (Optional) Defaults to `src/reproq_django/bin/reproq`.

## 5. Memory Considerations
Reproq Worker is extremely light. It typically uses less than 20MB of RAM, making it perfect for Render's **Free Tier** or **Starter** plans.

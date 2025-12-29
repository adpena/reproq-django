# Deployment Guide

Reproq is designed for stability in production environments. Follow these guidelines for a robust setup.

## 1. Systemd Configuration (Recommended)

Reproq provides an automated way to generate systemd service files. This is the preferred method for Linux servers.

```bash
python manage.py reproq systemd --concurrency 20
```

This will output two service files:
- `reproq-worker.service`: Manages the task execution nodes.
- `reproq-beat.service`: Manages the periodic task scheduler.

### Manual Installation
Copy the generated files to `/etc/systemd/system/` and enable them:

```bash
sudo cp *.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now reproq-worker
sudo systemctl enable --now reproq-beat
```

## 2. Environment Variables

The Go worker needs access to your database. It respects the following:

- `DATABASE_URL`: Standard PostgreSQL DSN (e.g., `postgres://user:pass@host:5432/dbname`).
- `WORKER_ID`: Unique name for the worker node (defaults to hostname).
- `PYTHONPATH`: Ensure your Django project is importable.

## 3. Worker Concurrency

The number of concurrent tasks a single worker process can handle is configurable.
- **CPU-bound tasks**: Set concurrency to `number of cores`.
- **IO-bound tasks**: Set concurrency higher (e.g., `20-50`).

```bash
python manage.py reproq worker --concurrency 50
```

## 4. Periodic Tasks (Beat)

**CRITICAL**: Only run **one instance** of the `beat` process per database. Running multiple instances will result in tasks being scheduled multiple times.

## 5. Reverse Proxy & Admin

The Reproq Admin uses standard Django Admin templates. Ensure your static files are collected and served correctly:

```bash
python manage.py collectstatic
```

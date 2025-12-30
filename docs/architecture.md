# Architecture Overview

Reproq follows a "split-brain" architecture to maximize both developer productivity and runtime performance.

## Core Components

1. **Reproq Django (The Manager)**
   - Responsible for task definition via the standard `@task` decorator.
   - Manages the PostgreSQL tables (`task_runs`, `periodic_tasks`, `reproq_workers`).
   - Provides the enqueuing logic and ensures tasks are written to the database with the correct `spec_hash`.
   - Offers the Django Admin dashboard for monitoring and manual control.

2. **Reproq Worker (The Runner)**
   - A standalone Go binary.
   - Uses a high-performance polling loop with `SKIP LOCKED` to claim tasks atomically.
   - Manages a pool of worker goroutines.
   - Executes tasks by invoking `python -m reproq_django.executor`.
   - Handles heartbeats to ensure the system knows it's alive and can recover from crashes.

3. **PostgreSQL (The Broker)**
   - Reproq is "Postgres-native."
   - The database is the single source of truth for task state, worker health, and periodic schedules.

## The Execution Flow

1. **Enqueue**: Python code calls `my_task.enqueue(args)`. Django inserts a row into `task_runs` with state `READY`.
2. **Claim**: The Go worker polls the database. It finds the `READY` task and atomically updates it to `RUNNING`, setting its `worker_id` and a lease timestamp.
3. **Execute**: The Go worker starts a Python sub-process. It passes the task payload via `stdin` as JSON.
4. **Heartbeat**: While the Python process is running, the Go worker periodically updates the task's lease in the database.
5. **Finalize**: Once the Python process finishes, it outputs the result (or traceback) to `stdout`. The Go worker captures this and updates the database state to `SUCCESSFUL` or `FAILED`.

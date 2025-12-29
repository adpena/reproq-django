# reproq-django

Django 6.0 Tasks backend for `reproq-worker` (Go).
Stores task runs in Postgres table `task_runs`. A separate worker service claims and executes them.

## Install
pip install -e .

## Django config
Add to INSTALLED_APPS:
- reproq_django

Configure TASKS backends to point at:
- reproq_django.backend.ReproqBackend

Then run:
python manage.py migrate

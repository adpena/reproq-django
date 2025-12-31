from __future__ import annotations

import sys
from pathlib import Path

import django
import pytest
from django.conf import settings


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


if not settings.configured:
    settings.configure(
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        INSTALLED_APPS=[
            "django.contrib.auth",
            "django.contrib.contenttypes",
            "reproq_django",
        ],
        TASKS={
            "default": {
                "BACKEND": "reproq_django.backend.ReproqBackend",
                "QUEUES": ["default", "test-queue", "q"],
            }
        },
        SECRET_KEY="test-secret",
    )
    django.setup()


@pytest.fixture(autouse=True, scope="session")
def _unblock_db_access(django_db_blocker):
    with django_db_blocker.unblock():
        yield

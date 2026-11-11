"""Optional, lossy worker hints. SQL rows and claims are the only queue authority."""

from functools import partial
import logging

from django.db import NotSupportedError, connections, transaction


CHANNEL = "reproq_wake_v1"
logger = logging.getLogger(__name__)


def validate_notifications(options: dict, alias: str) -> bool:
    enabled = options.get("WAKE_NOTIFICATIONS", False)
    if type(enabled) is not bool:
        raise TypeError("OPTIONS.WAKE_NOTIFICATIONS must be a boolean, True or False")
    if enabled and connections[alias].vendor != "postgresql":
        raise NotSupportedError("WAKE_NOTIFICATIONS requires a PostgreSQL queue database")
    return enabled


def schedule_notification(alias: str) -> None:
    # Retain only the alias, never another task/spec/result graph until commit.
    transaction.on_commit(partial(_send_notification, alias), using=alias, robust=True)


def _send_notification(alias: str) -> None:
    try:
        with connections[alias].cursor() as cursor:
            cursor.execute("SELECT pg_catalog.pg_notify(%s, %s)", [CHANNEL, ""])
    except Exception:
        # The enqueue is already committed. Never misreport it as rejected or
        # expose driver text, task data, connection parameters or credentials.
        # This deliberately contains every optional post-commit hint failure;
        # Django's outer robust-callback logger would expose exception text.
        logger.warning("Worker notification unavailable; committed work remains queued for bounded polling")

"""Hints improve latency; their failure must never change accepted queue work."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command
from django.db import DatabaseError, InterfaceError, NotSupportedError, transaction
from django.tasks import Task
from django.test import override_settings

from reproq_django.backend import ReproqBackend
from reproq_django.models import TaskRun
from reproq_django.signals import task_enqueued
from reproq_django.wakeup import CHANNEL, _send_notification, validate_notifications
from reproq_django.workflows import group


def hint_task(*args, **kwargs):
    return None


def task(backend="default"):
    return Task(func=hint_task, backend=backend, queue_name="default", priority=0, run_after=None)


@pytest.fixture
def rows():
    call_command("migrate", "reproq_django", verbosity=0)
    TaskRun.objects.all().delete()
    yield
    TaskRun.objects.all().delete()


def backend(enabled=True):
    return ReproqBackend(alias="default", params={"OPTIONS": {"WAKE_NOTIFICATIONS": enabled}})


@pytest.mark.parametrize("value", ["false", "true", 0, 1, None, [], {}])
def test_strict_option_rejected_before_enqueue(rows, value):
    with pytest.raises(TypeError, match="must be a boolean"):
        backend(value).enqueue(task(), (), {})
    assert TaskRun.objects.count() == 0


def test_postgres_required_only_when_enabled(rows):
    with pytest.raises(NotSupportedError, match="PostgreSQL"):
        backend().enqueue(task(), (), {})
    assert TaskRun.objects.count() == 0
    with patch("reproq_django.wakeup._send_notification") as send:
        backend(False).enqueue(task(), (), {})
        send.assert_not_called()


@pytest.fixture
def hints():
    # SQLite tests exercise commit/callback semantics, not PostgreSQL wire behavior.
    # Real LISTEN/NOTIFY has separate PostgreSQL integration coverage.
    with patch("reproq_django.wakeup.connections", {"default": SimpleNamespace(vendor="postgresql")}), \
            patch("reproq_django.wakeup._send_notification") as send:
        yield send


def test_enqueue_and_dedupe_signal_only_after_commit(rows, hints):
    b = backend()
    with transaction.atomic():
        first = b.enqueue(task(), (42,), {})
        second = b.enqueue(task(), (42,), {})
        assert first.id == second.id
        hints.assert_not_called()
    assert hints.call_count == 2  # Two explicit enqueue operations, one durable row.
    assert TaskRun.objects.count() == 1
    assert all(call.args == ("default",) for call in hints.call_args_list)


def test_rollback_discards_hint_and_row(rows, hints):
    with pytest.raises(RuntimeError, match="rollback"):
        with transaction.atomic():
            backend().enqueue(task(), (), {})
            raise RuntimeError("rollback")
    hints.assert_not_called()
    assert not TaskRun.objects.exists()


def test_bulk_emits_once_and_keeps_public_per_result_signals(rows, hints):
    observed = []
    def receiver(sender, task_result, **kwargs):
        observed.append(task_result.id)
    task_enqueued.connect(receiver, weak=False)
    try:
        with transaction.atomic():
            results = backend().bulk_enqueue([(task(), (i,), {}) for i in range(25)])
            hints.assert_not_called()
            assert not observed
        assert observed == [result.id for result in results]
        hints.assert_called_once_with("default")
    finally:
        task_enqueued.disconnect(receiver)


@pytest.mark.parametrize("flags", [(False, True), (True, False), (False, False)])
def test_mixed_backend_workflow_has_one_hint_if_any_opt_in(rows, hints, flags):
    tasks = {name: {"BACKEND": "reproq_django.backend.ReproqBackend", "OPTIONS": {"WAKE_NOTIFICATIONS": enabled}}
             for name, enabled in zip(("default", "other"), flags, strict=True)}
    with override_settings(TASKS=tasks):
        with transaction.atomic():
            results = group(task("default"), task("other")).enqueue()
            hints.assert_not_called()
        assert len(results) == 2
    assert hints.call_count == int(any(flags))


def test_invalid_later_workflow_backend_prevents_all_writes(rows, hints):
    tasks = {"default": {"BACKEND": "reproq_django.backend.ReproqBackend", "OPTIONS": {"WAKE_NOTIFICATIONS": True}},
             "other": {"BACKEND": "reproq_django.backend.ReproqBackend", "OPTIONS": {"WAKE_NOTIFICATIONS": "false"}}}
    with override_settings(TASKS=tasks), pytest.raises(TypeError, match="boolean"):
        group(task("default"), task("other")).enqueue()
    assert not TaskRun.objects.exists()
    hints.assert_not_called()


def test_public_receiver_failure_does_not_prevent_hint(rows, hints):
    def reject(**kwargs):
        raise RuntimeError("existing public signal behavior")
    task_enqueued.connect(reject, weak=False)
    try:
        with pytest.raises(RuntimeError, match="existing public signal"):
            backend().enqueue(task(), (), {})
        hints.assert_called_once_with("default")
        assert TaskRun.objects.count() == 1
    finally:
        task_enqueued.disconnect(reject)


@pytest.mark.parametrize("failure", [DatabaseError, InterfaceError, RuntimeError])
def test_send_has_empty_payload_and_never_logs_driver_text(caplog, failure):
    with patch("reproq_django.wakeup.connections") as connections:
        cursor = connections.__getitem__.return_value.cursor.return_value.__enter__.return_value
        _send_notification("selected")
        connections.__getitem__.assert_called_once_with("selected")
        cursor.execute.assert_called_once_with("SELECT pg_catalog.pg_notify(%s, %s)", [CHANNEL, ""])
        cursor.execute.side_effect = failure("password=do-not-log")
        _send_notification("selected")
    assert "committed work remains queued" in caplog.text
    assert "do-not-log" not in caplog.text


def test_default_option_is_disabled():
    assert validate_notifications({}, "unresolved-alias") is False


@pytest.mark.parametrize("failure", [DatabaseError, InterfaceError, RuntimeError])
def test_actual_commit_callback_failure_preserves_row_and_redacts_outer_log(rows, caplog, failure):
    connection = MagicMock(vendor="postgresql")
    connection.cursor.return_value.__enter__.return_value.execute.side_effect = failure("private-driver-text")
    with patch("reproq_django.wakeup.connections", {"default": connection}):
        with transaction.atomic():
            result = backend().enqueue(task(), (), {})
            connection.cursor.assert_not_called()
        assert TaskRun.objects.filter(pk=result.id).exists()
        assert connection.cursor.call_count == 1
    assert "committed work remains queued" in caplog.text
    assert "private-driver-text" not in caplog.text

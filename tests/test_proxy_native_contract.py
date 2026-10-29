"""Native result inspection keeps transport refs out of Python arguments."""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.tasks import TaskResultStatus

from reproq_django.backend import ReproqBackend
from reproq_django.models import QueueControl, TaskRun
from reproq_django.proxy import TaskResultProxy
from reproq_django.serialization import (
    TYPE_MARKER,
    DeserializationError,
    encode_args_kwargs,
)


@pytest.fixture
def result_databases(tmp_path):
    source, queue = "owned_proxy_source", "owned_proxy_queue"
    for alias in (source, queue):
        assert alias not in settings.DATABASES
        config = {
            **connections["default"].settings_dict,
            "NAME": str(tmp_path / f"{alias}.sqlite3"),
        }
        settings.DATABASES[alias] = config
        connections.databases[alias] = config
    try:
        with connections[source].schema_editor() as editor:
            editor.create_model(QueueControl)
        call_command("migrate", "reproq_django", database=queue, verbosity=0)
        yield source, queue
    finally:

        async def close_thread_connections():
            await sync_to_async(connections.close_all, thread_sensitive=True)()

        asyncio.run(close_thread_connections())
        for alias in (source, queue):
            connections[alias].close()
            del connections[alias]
            settings.DATABASES.pop(alias, None)
            connections.databases.pop(alias, None)


def stored_result(queue, args=(), kwargs=None, **fields):
    encoded_args, encoded_kwargs = encode_args_kwargs(args, kwargs or {})
    run = TaskRun.objects.using(queue).create(
        spec_json={"args": encoded_args, "kwargs": encoded_kwargs},
        spec_hash="a" * 64,
        **fields,
    )
    backend = ReproqBackend(alias="default", params={})
    return run, TaskResultProxy(str(run.pk), backend, db_alias=queue)


def test_source_model_same_pk_literal_envelopes_and_typed_arguments(result_databases):
    source, queue = result_databases
    original = QueueControl.objects.using(source).create(
        queue_name="same-pk", reason="source"
    )
    QueueControl.objects.using(queue).create(
        queue_name="same-pk", reason="wrong queue object"
    )
    literal = {TYPE_MARKER: "model", "database": queue, "pk": "same-pk"}
    duration = timedelta(days=-1, microseconds=31)
    _, proxy = stored_result(
        queue,
        (original, duration, literal),
        {TYPE_MARKER: "ordinary keyword", "literal": literal},
    )
    assert proxy.refresh() is None
    assert (proxy.decoded_args[0]._state.db, proxy.decoded_args[0].reason) == (
        source,
        "source",
    )
    assert proxy.decoded_args[1:] == [duration, literal]
    assert proxy.decoded_kwargs == {TYPE_MARKER: "ordinary keyword", "literal": literal}
    assert proxy.args[0]["database"] == source
    assert proxy.args[2][TYPE_MARKER] == "literal_dict"
    assert proxy.kwargs["literal"][TYPE_MARKER] == "literal_dict"


def test_deleted_source_error_is_cached_without_hiding_terminal_result(
    result_databases,
):
    source, queue = result_databases
    original = QueueControl.objects.using(source).create(queue_name="same-pk")
    QueueControl.objects.using(queue).create(
        queue_name="same-pk", reason="wrong queue object"
    )
    _, proxy = stored_result(
        queue, (original,), status="SUCCESSFUL", return_json={"answer": 0}
    )
    original.delete(using=source)
    # Refresh successfully binds the terminal row even though argument decoding
    # fails. Subsequent property access must not retry database lookup.
    assert proxy.refresh() is None
    with pytest.raises(DeserializationError):
        _ = proxy.decoded_args
    with patch(
        "reproq_django.serialization.decode_args_kwargs",
        side_effect=AssertionError("cached decode failure retried"),
    ):
        assert proxy.status is TaskResultStatus.SUCCESSFUL
        assert proxy.return_value == {"answer": 0}
        assert proxy.args[0]["database"] == source and proxy.kwargs == {}
        for attribute in ("decoded_args", "decoded_kwargs"):
            with pytest.raises(DeserializationError, match="could not be loaded"):
                getattr(proxy, attribute)


def test_bind_run_primes_arguments_without_reloading_queue_row(result_databases):
    source, queue = result_databases
    original = QueueControl.objects.using(source).create(queue_name="source")
    run, proxy = stored_result(queue, (original,))
    with patch.object(
        TaskResultProxy, "refresh", side_effect=AssertionError("queue row reloaded")
    ):
        assert proxy._bind_run(run) is proxy
        assert proxy.decoded_args[0]._state.db == source


@pytest.mark.parametrize("entrypoint", ["arefresh", "aget_result"])
def test_async_result_access_primes_model_decode_off_event_loop(
    result_databases, entrypoint
):
    source, queue = result_databases
    original = QueueControl.objects.using(source).create(
        queue_name="source", reason="business"
    )
    QueueControl.objects.using(queue).create(queue_name="source", reason="queue")
    run, proxy = stored_result(queue, (original,), {"duration": timedelta(seconds=7)})
    backend = ReproqBackend(alias="default", params={})

    async def inspect():
        if entrypoint == "aget_result":
            result = await backend.aget_result(f"{queue}:{run.pk}")
        else:
            assert await proxy.arefresh() is None
            result = proxy
        # Rich hydration is explicit and asynchronous; native refresh/get alone
        # need not query the business model database.
        await result.adecoded_args()
        await result.adecoded_kwargs()
        with patch(
            "reproq_django.serialization.decode_args_kwargs",
            side_effect=AssertionError("model decoded on event loop"),
        ):
            assert result.decoded_args[0]._state.db == source
            assert result.decoded_args[0].reason == "business"
            assert result.decoded_kwargs == {"duration": timedelta(seconds=7)}
            assert result.status is TaskResultStatus.READY

    asyncio.run(inspect())


@pytest.mark.parametrize(
    "raw,native",
    [
        ("READY", TaskResultStatus.READY),
        ("WAITING", TaskResultStatus.READY),
        ("RUNNING", TaskResultStatus.RUNNING),
        ("FAILED", TaskResultStatus.FAILED),
        ("CANCELLED", TaskResultStatus.FAILED),
        ("SUCCESSFUL", TaskResultStatus.SUCCESSFUL),
    ],
)
def test_supported_raw_statuses_project_to_actual_native_enum(raw, native):
    proxy = TaskResultProxy("1", SimpleNamespace(alias="default"))
    proxy._bind_run(
        SimpleNamespace(
            status=raw,
            spec_json={},
            spec_version=0,
            backend_alias="default",
            return_json=None,
        )
    )
    assert proxy.status is native and proxy.raw_status == raw


def test_unknown_future_status_remains_explicit_error():
    proxy = TaskResultProxy("1", SimpleNamespace(alias="default"))
    proxy._bind_run(
        SimpleNamespace(
            status="FUTURE_STATE",
            spec_json={},
            spec_version=0,
            backend_alias="default",
            return_json=None,
        )
    )
    with pytest.raises(RuntimeError, match="non-standard status"):
        _ = proxy.status
    assert proxy.raw_status == "FUTURE_STATE"

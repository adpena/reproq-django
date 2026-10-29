from datetime import timedelta
from io import StringIO
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.db import connection
from django.utils import timezone

from reproq_django.context import ExecutionClaim, LeaseLostError, TaskContext
from reproq_django.models import SchemaContract, TaskRun
from reproq_django.schema_contract import SchemaContractError, require_schema_contract
from reproq_django.specs import build_spec, spec_fields


@pytest.fixture(autouse=True)
def installed_schema():
    call_command("migrate", verbosity=0)
    TaskRun.objects.all().delete()
    yield
    TaskRun.objects.all().delete()


def running(**changes):
    fields = {**spec_fields(build_spec(task_path='owned.runtime.task')),
              "task_path": "owned.runtime.task", "dedupe_active": False,
              "status": "RUNNING", "attempts": 1,
              "worker_ids": ["same-worker"],
              "leased_by": "same-worker", "lease_token": uuid4(),
              "leased_until": timezone.now() + timedelta(minutes=1),
              "metadata_json": {"owner": "current"}}
    fields.update(changes)
    return TaskRun.objects.create(**fields)


def context_for(run):
    return TaskContext.from_claim(ExecutionClaim(
        result_id=str(run.pk), budget_attempt=run.attempts, expected_task_path=run.task_path,
        db_alias='default', worker_id=run.leased_by, lease_token=str(run.lease_token),
    ))


@pytest.mark.parametrize("change", ["token", "worker", "expired", "cancelled", "finished"])
def test_stale_context_cannot_load_or_overwrite_a_current_claim(change):
    run = running()
    stale = context_for(run)
    if change == "token":
        # The same worker ID and resettable attempt count do not grant ownership.
        TaskRun.objects.filter(pk=run.pk).update(lease_token=uuid4())
    elif change == "worker":
        TaskRun.objects.filter(pk=run.pk).update(leased_by="new-worker")
    elif change == "expired":
        TaskRun.objects.filter(pk=run.pk).update(leased_until=timezone.now() - timedelta(seconds=1))
    elif change == "cancelled":
        TaskRun.objects.filter(pk=run.pk).update(cancel_requested=True)
    else:
        TaskRun.objects.filter(pk=run.pk).update(status="SUCCESSFUL", lease_token=None)
    with pytest.raises(LeaseLostError):
        stale.load_metadata()
    stale.metadata['owner'] = 'stale'
    with pytest.raises(LeaseLostError):
        stale.save_metadata()
    run.refresh_from_db()
    assert run.metadata_json == {"owner": "current"}


def test_current_context_loads_and_saves_with_the_live_token():
    run = running()
    context = context_for(run)
    context.load_metadata()
    context.metadata["progress"] = 0
    context.save_metadata()
    run.refresh_from_db()
    assert run.metadata_json == {"owner": "current", "progress": 0}


def test_legacy_row_cannot_enter_the_current_executor_context():
    run = running(spec_version=0)
    with pytest.raises(LeaseLostError):
        context_for(run)


def test_legacy_operator_source_rejects_before_any_worker_process(monkeypatch):
    from reproq_django import operations

    run = running(spec_version=0)
    monkeypatch.setattr(operations, '_operation_context', lambda alias: pytest.fail('Legacy source invoked a worker probe'))
    with pytest.raises(operations.OperationError, match='Legacy/unknown'):
        operations.task_operation('retry', str(run.pk), using='default')


def test_workflow_retry_has_a_clear_refusal_before_any_worker_process(monkeypatch):
    from reproq_django import operations

    run = running(status='FAILED', workflow_id=uuid4(), leased_by=None,
                  leased_until=None, lease_token=None)
    monkeypatch.setattr(operations, '_operation_context', lambda alias: pytest.fail('Invalid workflow retry invoked a worker probe'))
    with pytest.raises(operations.OperationError, match='fresh workflow'):
        operations.task_operation('retry', str(run.pk), using='default')


def test_manual_reclaim_delegates_to_the_canonical_go_transition(monkeypatch):
    from reproq_django.management.commands.reproq import Command
    from django.db.models.query import QuerySet
    calls = []
    monkeypatch.setattr(Command, "_resolve_worker_bin", lambda self: ("owned-worker", "owned-worker", True))
    monkeypatch.setattr(Command, "get_dsn", lambda self, alias: "postgresql://owned@127.0.0.1/owned")
    monkeypatch.setattr('reproq_django.management.commands.reproq.require_worker_capabilities', lambda binary: None)
    def invoke(arguments, **options):
        calls.append((arguments, {"check": options["check"],
                                  "database_url": options["env"]["REPROQ_DATABASE_URL"]}))
    monkeypatch.setattr("subprocess.run", invoke)
    monkeypatch.setattr(QuerySet, "update", lambda *a, **kw: pytest.fail("SDK duplicated the Go reclaim transition"))
    call_command("reproq", "reclaim", "--older-than", "3m", "--limit", "1", "--dry-run", stdout=StringIO())
    assert len(calls) == 1 and calls[0][1]["check"] is True
    assert calls[0][1]["database_url"] == "postgresql://owned@127.0.0.1/owned"
    arguments = calls[0][0]
    assert arguments[:2] == ["owned-worker", "reclaim"]
    assert "--dsn" not in arguments and all("postgresql://" not in item for item in arguments)
    assert arguments[arguments.index("--older-than") + 1] == "180s"
    assert arguments[arguments.index("--limit") + 1] == "1" and "--dry-run" in arguments


def test_missing_contract_refuses_runtime_metadata_even_when_task_table_exists():
    run = running()
    marker = SchemaContract.objects.get()
    SchemaContract.objects.all().delete()
    try:
        with pytest.raises(SchemaContractError):
            require_schema_contract(connection)
        with pytest.raises(SchemaContractError):
            context_for(run).load_metadata()
    finally:
        marker.save(force_insert=True)

"""Exercise the installed schema's constraints, rather than migration text."""

from datetime import timedelta
import json
from uuid import uuid4
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.db import IntegrityError, connection, transaction
from django.db.migrations.recorder import MigrationRecorder
from django.utils import timezone

from reproq_django.models import RateLimit, SchemaContract, TaskRun, WorkflowRun
from reproq_django.schema_contract import (
    SCHEMA_MIGRATION,
    SCHEMA_VERSION,
    SchemaContractError,
    require_schema_contract,
)
from reproq_django.management.commands.reproq import Command


@pytest.fixture(autouse=True)
def installed_schema():
    call_command("migrate", verbosity=0)
    TaskRun.objects.all().delete()
    yield
    TaskRun.objects.all().delete()


def create_run(**kwargs):
    fields = {"spec_json": {"task_path": "owned.schema.fixture"},
              "task_path": "owned.schema.fixture", "spec_hash": "a" * 64}
    fields.update(kwargs)
    return TaskRun.objects.create(**fields)


def test_all_six_task_tables_have_managed_ownership():
    assert RateLimit._meta.managed and WorkflowRun._meta.managed
    assert {"task_runs", "periodic_tasks", "reproq_workers", "reproq_queue_controls",
            "rate_limits", "workflow_runs"} <= set(connection.introspection.table_names())
    assert require_schema_contract(connection)["version"] == SCHEMA_VERSION


def test_running_requires_fresh_token_owner_and_lease_and_completion_clears_token():
    run = create_run()
    future = timezone.now() + timedelta(minutes=1)
    for fields in [
        {"status": "RUNNING"},
        {"status": "RUNNING", "lease_token": uuid4(), "leased_by": "", "leased_until": future},
        {"status": "RUNNING", "lease_token": uuid4(), "leased_by": "owner"},
    ]:
        with pytest.raises(IntegrityError), transaction.atomic():
            TaskRun.objects.filter(pk=run.pk).update(**fields)
    TaskRun.objects.filter(pk=run.pk).update(
        status="RUNNING", lease_token=uuid4(), leased_by="owner", leased_until=future
    )
    with pytest.raises(IntegrityError), transaction.atomic():
        TaskRun.objects.filter(pk=run.pk).update(status="SUCCESSFUL")
    assert TaskRun.objects.filter(pk=run.pk).update(status="SUCCESSFUL", lease_token=None) == 1


def test_active_dedupe_option_keeps_truthful_hash_without_blocking_opt_out():
    create_run()
    with pytest.raises(IntegrityError), transaction.atomic():
        create_run()
    first = create_run(dedupe_active=False)
    second = create_run(dedupe_active=False)
    assert first.spec_hash == second.spec_hash == "a" * 64
    create_run(status="SUCCESSFUL")


def test_periodic_occurrence_identity_is_independent_of_active_hash_dedupe():
    when = timezone.now()
    create_run(periodic_name="daily", scheduled_for=when)
    create_run(periodic_name="daily", scheduled_for=when + timedelta(days=1))
    for fields in [
        {"periodic_name": "daily", "scheduled_for": when, "status": "SUCCESSFUL", "dedupe_active": False},
        {"periodic_name": "daily"},
        {"scheduled_for": when},
    ]:
        with pytest.raises(IntegrityError), transaction.atomic():
            create_run(**fields)


def test_database_defaults_preserve_legacy_unknown_spec_without_payload_rewrite():
    payload = {"task_path": "owned.schema.fixture", "kwargs": {"value": "é", "zero": 0}}
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO task_runs(spec_json,spec_hash,task_path) VALUES(%s,%s,%s)",
            [json.dumps(payload), "b" * 64, "owned.schema.fixture"],
        )
    run = TaskRun.objects.get(spec_hash="b" * 64)
    assert run.spec_json == payload and run.spec_hash == "b" * 64
    assert run.spec_version == 0 and run.canonical_spec_text is None
    assert run.dedupe_active and run.status == "READY" and run.metadata_json == {}
    assert run.worker_ids == [] and run.errors_json == [] and run.created_at is not None


def test_marker_alone_cannot_replace_recorded_forward_migration():
    history = MigrationRecorder(connection).migration_qs
    row = history.get(app="reproq_django", name=SCHEMA_MIGRATION)
    saved = {field.name: getattr(row, field.name) for field in row._meta.fields}
    row.delete()
    try:
        with pytest.raises(SchemaContractError):
            require_schema_contract(connection)
    finally:
        history.create(**saved)
    assert require_schema_contract(connection)["version"] == SCHEMA_VERSION


def test_unknown_contract_version_fails_closed():
    SchemaContract.objects.update(version=99)
    try:
        with pytest.raises(SchemaContractError):
            require_schema_contract(connection)
    finally:
        SchemaContract.objects.update(version=SCHEMA_VERSION)


@pytest.mark.parametrize("missing,duplicate", [
    ("0002_schema_contract_v1", None),
    (None, "0002_schema_contract_v1"),
    (SCHEMA_MIGRATION, "0002_schema_contract_v1"),
    ("0002_schema_contract_v1", SCHEMA_MIGRATION),
])
def test_each_required_history_row_must_exist_exactly_once(missing, duplicate):
    history = MigrationRecorder(connection).migration_qs
    with transaction.atomic():
        if missing:
            history.filter(app="reproq_django", name=missing).delete()
        if duplicate:
            history.create(app="reproq_django", name=duplicate)
        with pytest.raises(SchemaContractError):
            require_schema_contract(connection)
        transaction.set_rollback(True)
    assert require_schema_contract(connection)["version"] == SCHEMA_VERSION


def test_queue_dsn_preserves_quoted_credentials_and_postgres_options():
    from django.db.backends.postgresql.base import DatabaseWrapper
    from psycopg.conninfo import conninfo_to_dict

    config = {**connection.settings_dict, "ENGINE": "django.db.backends.postgresql",
              "NAME": "queue database", "USER": "owned user", "PASSWORD": "owned '\\ :& password",
              "HOST": "2001:db8::1", "PORT": "5432",
              "OPTIONS": {"options": "-c search_path=queues", "sslmode": "require"}}
    queue = DatabaseWrapper(config, alias="owned")
    with patch("reproq_django.management.commands.reproq.connections", {"owned": queue}):
        dsn = Command().get_dsn("owned")
    parts = conninfo_to_dict(dsn)
    assert parts["user"] == config["USER"] and parts["password"] == config["PASSWORD"]
    assert parts["dbname"] == config["NAME"] and parts["host"] == config["HOST"]
    assert parts["options"] == config["OPTIONS"]["options"] and parts["sslmode"] == "require"
    assert config["PASSWORD"] not in Command()._mask_dsn(dsn)


def test_doctor_database_target_comparison_distinguishes_database_and_schema():
    command = Command()
    assert command._dsn_target("postgresql://owned@localhost/queue") == command._dsn_target(
        "user=owned host=localhost port=5432 dbname=queue"
    )
    assert command._dsn_target("user=owned dbname=queue") != command._dsn_target(
        "user=owned dbname=application"
    )
    assert command._dsn_target("user=owned dbname=queue options='-c search_path=a'") != command._dsn_target(
        "user=owned dbname=queue options='-c search_path=b'"
    )

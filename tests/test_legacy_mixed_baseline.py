"""Exact observed Django/Go composition; nearby schemas remain unsupported."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from django.core.management import call_command
from django.db import connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder

from reproq_django._legacy_schema_profiles import LEGACY_SCHEMA_PROFILES
from reproq_django._schema_v1_catalog import SchemaAdoptionError, validate_pre_contract_v1
from reproq_django.schema_adoption import RAW_RECONCILED_LEDGER, inspect_legacy_schema
from reproq_django.schema_inspection import catalog_sha256_v1


def mixed_catalog():
    return deepcopy(next(p["catalog"] for p in LEGACY_SCHEMA_PROFILES
                         if p["name"] == "legacy-django-core-go-aux-v1"))


def test_exact_source_composition_matches_observed_catalog_and_validates():
    catalog = mixed_catalog()
    assert catalog_sha256_v1(catalog) == (
        "b23eeea7f18b9bf81b7152922c3c9acf7be25e52b604d3085e5bf4a61a5c941a"
    )
    with patch("reproq_django._schema_v1_catalog.read_legacy_catalog", return_value=catalog):
        validate_pre_contract_v1(SimpleNamespace(vendor="postgresql"))


@pytest.mark.parametrize("mutation", [
    "missing_index", "extra_index", "predicate", "invalid_index", "column_type",
    "auxiliary_default", "trigger_body", "sequence", "extra_constraint",
])
def test_mixed_baseline_requires_every_exact_object(mutation):
    catalog = mixed_catalog()
    index = next(row for row in catalog["indexes"] if row[1] == "idx_task_runs_claim")
    if mutation == "missing_index":
        catalog["indexes"].remove(index)
    elif mutation == "extra_index":
        catalog["indexes"].append(["task_runs", "unexpected_index", "unexpected SQL", True, True, False])
    elif mutation == "predicate":
        index[2] = index[2].replace("READY", "RUNNING")
    elif mutation == "invalid_index":
        index[3] = False
    elif mutation == "column_type":
        next(row for row in catalog["columns"] if row[:2] == ["task_runs", "spec_hash"])[2] = "bpchar"
    elif mutation == "auxiliary_default":
        next(row for row in catalog["columns"] if row[:2] == ["rate_limits", "last_refilled_at"])[5] = None
    elif mutation == "trigger_body":
        catalog["triggers"][0][-1] = "0" * 64
    elif mutation == "sequence":
        catalog["sequences"][0][5] = 2
    else:
        catalog["constraints"].append(["task_runs", "unexpected_check", "c", "CHECK (false)", True, False, False])
    with patch("reproq_django._schema_v1_catalog.read_legacy_catalog", return_value=catalog):
        with pytest.raises(SchemaAdoptionError):
            validate_pre_contract_v1(SimpleNamespace(vendor="postgresql"))


def test_existing_baseline_history_plans_only_normal_forward_migrations():
    call_command("migrate", verbosity=0)
    recorder = MigrationRecorder(connection)
    with transaction.atomic():
        recorder.migration_qs.filter(app="reproq_django").delete()
        for name in sorted(RAW_RECONCILED_LEDGER):
            recorder.record_applied("reproq_django", name)
        with patch("reproq_django.schema_adoption.read_legacy_catalog", return_value=mixed_catalog()):
            inspected = inspect_legacy_schema(connection)
        assert inspected["compatible_catalog"] and inspected["compatible_ledger"]
        assert inspected["profile"] == "legacy-django-core-go-aux-v1"
        executor = MigrationExecutor(connection)
        executor.loader.check_consistent_history(connection)
        plan = executor.migration_plan([("reproq_django", "0003_periodic_enqueue_v1")])
        assert [(migration.name, backwards) for migration, backwards in plan] == [
            ("0002_schema_contract_v1", False), ("0003_periodic_enqueue_v1", False),
        ]
        transaction.set_rollback(True)

"""Explicit, guarded reconciliation of known pre-contract PostgreSQL schemas.

This module never creates or repairs queue tables. All forward queue DDL belongs
to committed Django migrations. Historical profiles are validation evidence,
not an alternate schema definition or a table-existence adoption heuristic.
"""

from datetime import datetime, timezone
from pathlib import Path

from django.db import transaction
from django.db.migrations.recorder import MigrationRecorder

from ._legacy_schema_profiles import LEGACY_SCHEMA_PROFILES
from .schema_audit import write_receipt_v1 as _write_receipt
from .schema_inspection import catalog_sha256_v1 as _digest
from ._schema_v1_catalog import (
    QUEUE_TABLES, SchemaAdoptionError, read_legacy_catalog, require_public_relations_v1,
)

ADOPTION_LOCK = 0x5250524F51534348
BASELINE = "0001_baseline"
COVERED_REPLACEMENTS = (
    "0001_initial", "0002_taskrun_indexes",
    "0003_rename_idx_task_runs_status_task_runs_status_26197d_idx_and_more",
)
LEGACY_FAILURE_MIGRATION = "0002_add_failure_fields"
LEGACY_LEDGER = frozenset(("0001_initial", "0002_taskrun_indexes", LEGACY_FAILURE_MIGRATION))
RECONCILED_LEDGER = LEGACY_LEDGER | frozenset(COVERED_REPLACEMENTS) | {BASELINE}
RAW_RECONCILED_LEDGER = frozenset(COVERED_REPLACEMENTS) | {BASELINE}
ALLOWED_LEDGERS = (frozenset(), LEGACY_LEDGER, RECONCILED_LEDGER, RAW_RECONCILED_LEDGER)


def inspect_legacy_schema(connection):
    catalog = read_legacy_catalog(connection)
    digest = _digest(catalog)
    profile = next((p for p in LEGACY_SCHEMA_PROFILES if p["catalog"] == catalog), None)
    recorder = MigrationRecorder(connection)
    # applied_migrations() returns a dictionary and silently collapses duplicate
    # app/name records. Inspect actual history rows before accepting an allowlist.
    names = list(recorder.migration_qs.filter(app="reproq_django").values_list("name", flat=True)) if recorder.has_table() else []
    ledger = frozenset(names)
    return {"catalog_sha256": digest, "profile": profile["name"] if profile else None,
            "compatible_catalog": profile is not None, "ledger": sorted(ledger),
            "compatible_ledger": len(names) == len(ledger) and ledger in ALLOWED_LEDGERS}


def adopt_legacy_schema(connection, *, expected_catalog_sha256, audit_output):
    """Reconcile only fully verified known history, with a fsynced audit receipt.

    The published squash names a third replacement member that was never a
    committed migration. It is a compatibility marker in that published squash;
    this adoption does not invent historical code or pretend to execute it.
    """
    if connection.vendor != "postgresql":
        raise SchemaAdoptionError("Legacy schema adoption supports PostgreSQL only.")
    if connection.in_atomic_block or not connection.get_autocommit():
        raise SchemaAdoptionError("Adoption requires its own outermost autocommit transaction boundary.")
    if not isinstance(expected_catalog_sha256, str) or len(expected_catalog_sha256) != 64:
        raise SchemaAdoptionError("Supply the exact inspected catalog SHA-256.")
    path = Path(audit_output).expanduser().resolve()
    if path.exists() or not path.parent.is_dir():
        raise SchemaAdoptionError("Audit output must be a new file in an existing directory.")
    receipt = None
    receipt_created = False
    transaction_body_completed = False
    try:
        with transaction.atomic(using=connection.alias, durable=True):
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", [ADOPTION_LOCK])
                require_public_relations_v1(connection, allow_missing=True)
                tables = set(connection.introspection.table_names(cursor))
                if not set(QUEUE_TABLES).issubset(tables):
                    raise SchemaAdoptionError("A complete known six-table legacy schema is required.")
                for table in QUEUE_TABLES:
                    cursor.execute("LOCK TABLE public." + connection.ops.quote_name(table) + " IN SHARE ROW EXCLUSIVE MODE")
                if "django_migrations" in tables:
                    cursor.execute("LOCK TABLE public.django_migrations IN SHARE ROW EXCLUSIVE MODE")
            inspected = inspect_legacy_schema(connection)
            if inspected["catalog_sha256"] != expected_catalog_sha256:
                raise SchemaAdoptionError("Catalog changed since inspection; no history was adopted.")
            if not inspected["compatible_catalog"] or not inspected["compatible_ledger"]:
                raise SchemaAdoptionError("Unknown or incompatible legacy schema/history; no history was adopted.")
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM public.task_runs WHERE status='RUNNING'")
                if cursor.fetchone()[0]:
                    raise SchemaAdoptionError("Drain RUNNING tasks and stop legacy workers before adoption.")
                cursor.execute("SELECT COUNT(*) FROM public.task_runs WHERE task_path IS NULL OR task_path=''")
                if cursor.fetchone()[0]:
                    raise SchemaAdoptionError("Legacy task_path values violate the intended check; explicit repair is required.")
                cursor.execute("SELECT COUNT(*) FROM public.task_runs WHERE length(spec_hash) <> 64")
                if cursor.fetchone()[0]:
                    raise SchemaAdoptionError("Legacy hashes have an invalid length; no historical hash was rewritten.")
            before = frozenset(inspected["ledger"])
            after = before | frozenset(COVERED_REPLACEMENTS) | {BASELINE}
            receipt = {"action": "reproq-guarded-legacy-schema-adoption-v1",
                       "state": "prepared", "observed_utc": datetime.now(timezone.utc).isoformat(),
                       "database_alias": connection.alias, "catalog_sha256": expected_catalog_sha256,
                       "profile": inspected["profile"], "ledger_before": sorted(before),
                       "ledger_after": sorted(after), "added_history": sorted(after-before),
                       "historical_payloads_rewritten": False,
                       "note": "A prepared receipt after interruption requires independent ledger verification; it does not assert transaction commit."}
            _write_receipt(path, receipt, exclusive=True)
            receipt_created = True
            recorder = MigrationRecorder(connection)
            # Only Django's own migration-history infrastructure may be created
            # here; queue-table DDL belongs exclusively to forward migrations.
            recorder.ensure_schema()
            for name in sorted(after-before):
                recorder.record_applied("reproq_django", name)
            transaction_body_completed = True
    except Exception as exc:
        if receipt_created:
            # Losing the COMMIT acknowledgement does not prove rollback. Even
            # a body failure may encounter an unconfirmed rollback on disconnect.
            receipt["state"] = "commit_unknown" if transaction_body_completed else "unconfirmed"
            _write_receipt(path, receipt)
            raise SchemaAdoptionError(
                "Adoption outcome is unconfirmed; independently verify the audit "
                "receipt and database ledger before continuing."
            ) from exc
        raise
    receipt["state"] = "committed"
    try:
        _write_receipt(path, receipt)
    except OSError as exc:
        raise SchemaAdoptionError(
            "History committed but final audit update failed; independently verify "
            "the prepared receipt and database ledger before continuing."
        ) from exc
    return receipt

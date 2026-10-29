"""Use actual Django history rows; only PostgreSQL catalog inspection is stubbed."""

from unittest.mock import patch

from django.core.management import call_command
from django.db import connection, transaction
from django.db.migrations.recorder import MigrationRecorder

from reproq_django._legacy_schema_profiles import LEGACY_SCHEMA_PROFILES
from reproq_django.schema_adoption import LEGACY_LEDGER, inspect_legacy_schema


def test_duplicate_legacy_history_is_not_silently_collapsed():
    call_command("migrate", verbosity=0)
    recorder = MigrationRecorder(connection)
    with transaction.atomic():
        recorder.migration_qs.filter(app="reproq_django").delete()
        for name in sorted(LEGACY_LEDGER):
            recorder.record_applied("reproq_django", name)
        with patch("reproq_django.schema_adoption.read_legacy_catalog",
                   return_value=LEGACY_SCHEMA_PROFILES[0]["catalog"]):
            assert inspect_legacy_schema(connection)["compatible_ledger"]
            recorder.record_applied("reproq_django", sorted(LEGACY_LEDGER)[0])
            assert not inspect_legacy_schema(connection)["compatible_ledger"]
        transaction.set_rollback(True)

"""One immutable occurrence primitive for the Go and optional pg_cron providers."""

import hashlib
from importlib import resources

from django.db import migrations, models
from django.db.models.functions import Now


SQL_RESOURCE_SHA256 = "af9941a6daf2561b8160e1382923b8776445e9a81a6cfb3c83d51683809d2098"
FUNCTION_BODY_SHA256 = "eca65eaf6d1dbb5e4afc256f950e3049b6f88439f1be9abd3725096c02f4be8b"
FUNCTION_SIGNATURE = "public.reproq_enqueue_periodic_occurrence(text,timestamp with time zone,text)"
LEGACY_589_PROSRC_SHA256 = "cc34ef2e6cb7e9de727fa0a773e7c00b553f9e16fb5adc4d0e374b301ed7e174"
CAPABILITIES_V2 = ["lease-token-v1", "canonical-spec-v1", "periodic-occurrence-v1",
                   "managed-six-tables-v1", "periodic-enqueue-v1", "scheduler-provider-v1"]


def require_completed_v1(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor == "postgresql":
        from reproq_django._schema_v1_catalog import require_public_relations_v1
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [0x5250524F51534348])
        require_public_relations_v1(connection)
    marker = apps.get_model("reproq_django", "SchemaContract")
    rows = list(marker.objects.using(connection.alias).values(
        "singleton_id", "version", "contract_id", "migration_identity", "capabilities"
    ))
    expected = {"singleton_id": 1, "version": 1, "contract_id": "reproq-schema-v1",
                "migration_identity": "reproq_django.0002_schema_contract_v1",
                "capabilities": CAPABILITIES_V2[:4]}
    prefix = "public." if connection.vendor == "postgresql" else ""
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM " + prefix + "django_migrations WHERE app=%s AND name=%s",
                       ["reproq_django", "0002_schema_contract_v1"])
        recorded = cursor.fetchone()[0]
    if rows != [expected] or recorded != 1:
        raise RuntimeError("Complete the prior ReproQ schema contract before migration 0003.")


def install_occurrence_primitive(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        return
    from reproq_django._schema_v1_catalog import require_public_relations_v1

    require_public_relations_v1(connection)
    content = resources.files("reproq_django.resources").joinpath("periodic_enqueue_v1.sql").read_bytes()
    if hashlib.sha256(content).hexdigest() != SQL_RESOURCE_SHA256:
        raise RuntimeError("The immutable periodic enqueue SQL resource does not match migration 0003.")
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [0x5250524F51534348])
        cursor.execute("SELECT pg_catalog.to_regprocedure('public.reproq_enqueue_periodic_task(text)')::oid")
        legacy_oid = cursor.fetchone()[0]
        if legacy_oid is not None:
            cursor.execute(
                "SELECT p.prosrc,l.lanname,p.prosecdef,p.provolatile,p.proparallel,p.proconfig,"
                "pg_catalog.pg_get_function_result(p.oid),p.pronargdefaults,p.proargnames "
                "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE p.oid=%s",
                [legacy_oid],
            )
            body, language, definer, volatility, parallel, config, result, defaults, arguments = cursor.fetchone()
            if (hashlib.sha256(body.encode("utf-8")).hexdigest() != LEGACY_589_PROSRC_SHA256
                    or language != "plpgsql" or definer or volatility != "v" or parallel != "u"
                    or config is not None or result != "void" or defaults or arguments != ["task_name"]):
                raise RuntimeError("Unknown legacy periodic function ownership; inspect it explicitly before migration.")
            cursor.execute(
                "SELECT count(*) FROM pg_catalog.pg_depend WHERE "
                "(refclassid='pg_catalog.pg_proc'::regclass AND refobjid=%s) OR "
                "(classid='pg_catalog.pg_proc'::regclass AND objid=%s AND deptype='e')",
                [legacy_oid, legacy_oid],
            )
            if cursor.fetchone()[0]:
                raise RuntimeError("Legacy periodic function has dependencies that require explicit retirement.")
            cursor.execute("SELECT pg_catalog.to_regclass('cron.job')::oid")
            jobs_oid = cursor.fetchone()[0]
            if jobs_oid is not None:
                cursor.execute(
                    "SELECT count(*) FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_extension e ON e.oid=d.refobjid "
                    "WHERE d.classid='pg_catalog.pg_class'::regclass AND d.objid=%s AND d.deptype='e' AND e.extname='pg_cron'",
                    [jobs_oid],
                )
                if cursor.fetchone()[0] != 1:
                    raise RuntimeError("Unknown cron.job ownership; no scheduler metadata was modified.")
                cursor.execute(
                    "SELECT count(*) FROM cron.job WHERE database=pg_catalog.current_database() "
                    "AND command ILIKE %s", ["%reproq_enqueue_periodic_task%"],
                )
                if cursor.fetchone()[0]:
                    raise RuntimeError("Unschedule known legacy ReproQ jobs explicitly with reproq pg-cron --remove before migration.")
            cursor.execute("DROP FUNCTION public.reproq_enqueue_periodic_task(text)")
        # CREATE, not CREATE OR REPLACE: an unknown object cannot be overwritten.
        cursor.execute(content.decode("utf-8"))


def establish_contract_v2(apps, schema_editor):
    marker = apps.get_model("reproq_django", "SchemaContract")
    changed = marker.objects.using(schema_editor.connection.alias).filter(
        singleton_id=1, version=1, contract_id="reproq-schema-v1",
        migration_identity="reproq_django.0002_schema_contract_v1",
    ).update(version=2, contract_id="reproq-schema-v2",
             migration_identity="reproq_django.0003_periodic_enqueue_v1",
             capabilities=CAPABILITIES_V2, applied_at=Now())
    if changed != 1:
        raise RuntimeError("The prior completed ReproQ schema contract is required before migration 0003.")


class Migration(migrations.Migration):
    dependencies = [("reproq_django", "0002_schema_contract_v1")]

    operations = [
        migrations.RunPython(require_completed_v1),
        migrations.AddField("periodictask", "scheduler_provider", models.TextField(
            default="unassigned", db_default="unassigned",
            choices=[("unassigned", "Unassigned"), ("go", "Go"), ("pg_cron", "pg_cron")],
        )),
        migrations.AddField("taskrun", "periodic_provider", models.TextField(null=True, blank=True)),
        migrations.AddConstraint("periodictask", models.CheckConstraint(
            condition=models.Q(scheduler_provider__in=["unassigned", "go", "pg_cron"]),
            name="reproq_scheduler_provider_valid",
        )),
        migrations.AddConstraint("taskrun", models.CheckConstraint(
            condition=models.Q(periodic_provider__isnull=True) | models.Q(periodic_provider__in=["go", "pg_cron"]),
            name="reproq_periodic_provider_valid",
        )),
        migrations.RunPython(install_occurrence_primitive),
        # Written last. PostgreSQL function creation and the marker are atomic;
        # historical payloads, hashes and operator schedule choices are untouched.
        migrations.RunPython(establish_contract_v2),
    ]

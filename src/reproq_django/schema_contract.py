"""The compatibility contract established by committed Django migrations."""

SCHEMA_VERSION = 2
SCHEMA_CONTRACT_ID = "reproq-schema-v2"
SCHEMA_MIGRATION = "0003_periodic_enqueue_v1"
SCHEMA_MIGRATION_ID = f"reproq_django.{SCHEMA_MIGRATION}"
SCHEMA_CAPABILITIES = (
    "lease-token-v1",
    "canonical-spec-v1",
    "periodic-occurrence-v1",
    "managed-six-tables-v1",
    "periodic-enqueue-v1",
    "scheduler-provider-v1",
)


class SchemaContractError(RuntimeError):
    pass


def require_schema_contract(connection):
    """Fail closed when this database has not completed the supported migration."""
    if connection.vendor == "postgresql":
        from ._schema_v1_catalog import SchemaAdoptionError, require_public_relations_v1

        try:
            require_public_relations_v1(connection)
        except SchemaAdoptionError as exc:
            raise SchemaContractError(str(exc)) from exc
    with connection.cursor() as cursor:
        tables = set(connection.introspection.table_names(cursor))
        if not {"reproq_schema_contract", "django_migrations"}.issubset(tables):
            raise SchemaContractError(
                "ReproQ schema contract is missing; apply committed Django migrations "
                "on the selected queue database (legacy databases require explicit adoption)."
            )
        prefix = '"public".' if connection.vendor == "postgresql" else ""
        cursor.execute(
            "SELECT singleton_id, version, contract_id, migration_identity, capabilities "
            "FROM " + prefix + "reproq_schema_contract"
        )
        rows = cursor.fetchall()
        cursor.execute(
            "SELECT COUNT(*) FROM " + prefix + "django_migrations WHERE app = %s AND name = %s",
            ["reproq_django", SCHEMA_MIGRATION],
        )
        recorded = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM " + prefix + "django_migrations WHERE app = %s AND name = %s",
            ["reproq_django", "0002_schema_contract_v1"],
        )
        baseline_recorded = cursor.fetchone()[0]
    if len(rows) != 1:
        raise SchemaContractError("ReproQ requires exactly one schema contract marker.")
    singleton_id, version, contract_id, migration_id, capabilities = rows[0]
    # SQLite's cursor returns JSON as text; PostgreSQL decodes it through psycopg.
    if isinstance(capabilities, str):
        import json

        try:
            capabilities = json.loads(capabilities)
        except ValueError as exc:
            raise SchemaContractError("ReproQ schema capabilities are invalid.") from exc
    if (
        singleton_id != 1
        or version != SCHEMA_VERSION
        or contract_id != SCHEMA_CONTRACT_ID
        or migration_id != SCHEMA_MIGRATION_ID
        or not isinstance(capabilities, list)
        or capabilities != list(SCHEMA_CAPABILITIES)
        or recorded != 1
        or baseline_recorded != 1
    ):
        raise SchemaContractError(
            "Unsupported or incomplete ReproQ schema contract; apply the reviewed "
            "compatible Django migration before starting a worker or scheduler."
        )
    if connection.vendor == "postgresql":
        from importlib import import_module
        authority = import_module("reproq_django.migrations.0003_periodic_enqueue_v1")
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT l.lanname,p.prosecdef,p.provolatile,p.proparallel,p.proconfig,"
                "pg_catalog.pg_get_function_result(p.oid),"
                "pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex'),"
                "p.pronargdefaults,p.proargnames "
                "FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
                "WHERE p.oid=pg_catalog.to_regprocedure(%s)", [authority.FUNCTION_SIGNATURE],
            )
            function = cursor.fetchone()
        if (function is None or function != ("plpgsql", False, "v", "u",
                ["search_path=pg_catalog, pg_temp"], "TABLE(result_id bigint, inserted boolean)",
                authority.FUNCTION_BODY_SHA256, 0,
                ["p_name", "p_scheduled_for", "p_provider", "result_id", "inserted"])):
            raise SchemaContractError("The immutable periodic enqueue primitive or prior migration history is missing or incompatible.")
    return {"version": version, "contract_id": contract_id, "migration_identity": migration_id,
            "capabilities": capabilities}

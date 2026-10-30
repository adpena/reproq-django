"""Frozen v1 semantic catalog reader and legacy auxiliary adoption guard.

Historical migration behavior depends on this module; future contract versions
must add versioned helpers rather than changing these v1 semantics.
"""

from . import schema_inspection as inspection

from ._legacy_schema_profiles import DJANGO_BASELINE_PROFILE, LEGACY_SCHEMA_PROFILES

QUEUE_TABLES = (
    "periodic_tasks", "rate_limits", "reproq_queue_controls", "reproq_workers",
    "task_runs", "workflow_runs",
)
CONTRACT_RELATIONS_V1 = (*QUEUE_TABLES, "reproq_schema_contract", "django_migrations")


class SchemaAdoptionError(RuntimeError):
    pass


def _inspection_error(exc):
    # Keep the published queue-v1 exception surface while sharing inspection.
    if exc.code == "namespace":
        return SchemaAdoptionError("Only the public queue schema is qualified for contract v1.")
    if exc.code == "relation":
        return SchemaAdoptionError(
            f"Queue relation {exc.relation!r} must resolve to an ordinary permanent public table; "
            "shadow, temporary, view and missing relations cannot certify contract v1."
        )
    return SchemaAdoptionError(str(exc))


def require_public_relations_v1(connection, *, allow_missing=False):
    try:
        inspection.require_public_relations_v1(
            connection, relations=CONTRACT_RELATIONS_V1, allow_missing=allow_missing
        )
    except inspection.SchemaInspectionError as exc:
        raise _inspection_error(exc) from exc


def read_legacy_catalog(connection):
    if connection.vendor != "postgresql":
        raise SchemaAdoptionError("Legacy schema adoption supports PostgreSQL only.")
    try:
        return inspection.read_public_catalog_v1(
            connection, tables=QUEUE_TABLES,
            extra_relations=("reproq_schema_contract", "django_migrations"),
        )
    except inspection.SchemaInspectionError as exc:
        raise _inspection_error(exc) from exc


def _subset(catalog, tables):
    return {
        key: [row for row in rows if row[0] in tables
              or (key == "inheritance" and row[1] == "public" and row[2] in tables)]
        for key, rows in catalog.items()
    }


def validate_pre_contract_v1(connection):
    """Do not trust historical migration records as a physical schema proof."""
    actual = read_legacy_catalog(connection)
    core = set(QUEUE_TABLES) - {"workflow_runs", "rate_limits"}
    known = [*LEGACY_SCHEMA_PROFILES, DJANGO_BASELINE_PROFILE]
    if not any(_subset(actual, core) == _subset(profile["catalog"], core) for profile in known):
        raise SchemaAdoptionError(
            "Core queue tables do not match a supported complete pre-contract catalog; "
            "no schema contract can be established."
        )
    present = {row[0] for row in actual["tables"]}
    for table in {"workflow_runs", "rate_limits"} & present:
        if not any(_subset(actual, {table}) == _subset(profile["catalog"], {table})
                   for profile in LEGACY_SCHEMA_PROFILES):
            raise SchemaAdoptionError(
                f"Existing {table} does not match a complete v1 legacy catalog; "
                "no schema contract can be established."
            )

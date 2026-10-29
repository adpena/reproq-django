"""Optional native scheduler adapter; migrations own all queue/function DDL.

pg_cron only calls the shared occurrence primitive with the stored SDK spec.
Its occurrence timestamp is the UTC minute of execution, not an invented
nominal tick for jobs the native scheduler delayed or missed.
"""

import hashlib
import re

from django.db import transaction
from psycopg import sql

from .schema_contract import require_schema_contract
from .schema_inspection import require_public_relations_v1
from .recurring import parse_schedule, periodic_definition_status


class PgCronError(ValueError):
    pass


def is_available(cursor):
    cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname='pg_cron')")
    return cursor.fetchone()[0]


def occurrence_command(name, connection=None):
    return sql.SQL(
        "SELECT * FROM public.reproq_enqueue_periodic_occurrence({}, "
        "date_trunc('minute', statement_timestamp()), 'pg_cron');"
    ).format(sql.Literal(name)).as_string(connection)


def job_name(prefix, name, database=''):
    # Always include the original name digest: sanitization alone can collide.
    suffix = hashlib.sha256((database + '\x00' + name).encode('utf-8')).hexdigest()[:16]
    slug = re.sub(r'[^a-zA-Z0-9_]+', '_', name).strip('_') or 'task'
    return f'{prefix}_{slug[:45-len(prefix)]}_{suffix}'


def _legacy_job_name(prefix, name):
    """Recognize only the released SDK's exact historical named-job spelling."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]+', '_', name).strip('_')
    if not cleaned:
        cleaned = hashlib.sha256(name.encode('utf-8')).hexdigest()[:8]
    base = f'{prefix}_{cleaned}'
    if len(base) <= 63:
        return base
    return f'{base[:54]}_{hashlib.sha256(base.encode("utf-8")).hexdigest()[:8]}'


def _same_zone(left, right):
    utc = {'UTC', 'GMT', 'Etc/UTC', 'Etc/GMT'}
    return left == right or (left in utc and right in utc)


def native_schedule(expression, native_zone):
    schedule, zone = parse_schedule(expression)
    if not _same_zone(zone.key, native_zone):
        raise PgCronError(
            f'Schedule zone {zone.key!r} differs from native cron.timezone '
            f'{native_zone!r}; configure PostgreSQL explicitly or use the Go provider.'
        )
    return schedule


def reconcile(connection, *, prefix='reproq_periodic', remove=False, dry_run=False):
    """Reconcile only known jobs owned by this database and current DB role.

    Removal deliberately works before the v2 migration, so operators can stop
    exact known legacy jobs before the migration retires their old function.
    Unknown commands or names fail before the first unschedule/schedule write.
    """
    if connection.vendor != 'postgresql':
        raise PgCronError('pg_cron requires PostgreSQL.')
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,31}', prefix):
        raise PgCronError('Use a job prefix of 1–32 letters, digits or underscores, starting with a letter.')
    with transaction.atomic(using=connection.alias):
        require_public_relations_v1(connection, relations=('periodic_tasks',))
        with connection.cursor() as cursor:
            if not is_available(cursor):
                raise PgCronError('A database administrator must install/configure pg_cron first.')
            cursor.execute("SELECT pg_catalog.to_regprocedure('cron.schedule(text,text,text)') IS NOT NULL")
            if not cursor.fetchone()[0]:
                raise PgCronError('This adapter requires the named-job pg_cron API.')
            if not remove:
                require_schema_contract(connection)
            cursor.execute('SELECT pg_catalog.pg_advisory_xact_lock(%s)', [0x5250524F5143524F])
            cursor.execute('LOCK TABLE public.periodic_tasks IN SHARE ROW EXCLUSIVE MODE')
            cursor.execute("SELECT current_database(),COALESCE(current_setting('cron.host',true),'localhost'),current_setting('port')::integer")
            database, native_host, native_port = cursor.fetchone()
            if remove:
                cursor.execute('SELECT name FROM public.periodic_tasks ORDER BY name')
                definitions = [{'name': row[0]} for row in cursor.fetchall()]
            else:
                cursor.execute(
                    'SELECT name,cron_expr,enabled,scheduler_provider,spec_version '
                    'FROM public.periodic_tasks ORDER BY name'
                )
                definitions = [dict(zip(('name', 'cron_expr', 'enabled', 'provider', 'version'), row))
                               for row in cursor.fetchall()]
                from .models import PeriodicTask
                from .specs import read_periodic_spec
                # Validate Django intent before installing native jobs; stream
                # source rows so large argument payloads are not retained for
                # every definition in the reconciliation plan.
                for row in PeriodicTask.objects.using(connection.alias).filter(
                    enabled=True, scheduler_provider='pg_cron', spec_version=1,
                ).only('canonical_spec_text','canonical_spec_hash','spec_version',
                       'task_path','queue_name','priority','concurrency_key',
                       'concurrency_limit','max_attempts').iterator(chunk_size=16):
                    read_periodic_spec(row)
            known = {}
            for definition in definitions:
                name = definition['name']
                new_command = occurrence_command(name, connection.connection)
                legacy_command = "SELECT reproq_enqueue_periodic_task('" + name.replace("'", "''") + "');"
                for label, command in ((job_name(prefix, name, database), new_command),
                                       (_legacy_job_name(prefix, name), legacy_command)):
                    known.setdefault(label, set()).add(command)
            cursor.execute(
                'SELECT jobid,jobname,command,database,nodename,nodeport FROM cron.job '
                'WHERE username=current_user '
                'AND left(jobname,%s)=%s ORDER BY jobid FOR UPDATE',
                [len(prefix) + 1, prefix + '_'],
            )
            existing = []
            for jobid, label, command, job_database, node_host, node_port in cursor.fetchall():
                # Named-job uniqueness is per-role across databases. A foreign
                # row must never be silently retargeted by cron.schedule().
                if job_database != database:
                    if label in known:
                        raise PgCronError(f'Job label {label!r} belongs to another database; no jobs were changed.')
                    continue
                if node_host != native_host or node_port != native_port:
                    raise PgCronError(f'Job {label!r} has a different native connection route; no jobs were changed.')
                if command not in known.get(label, ()):
                    raise PgCronError(
                        f'Unknown job in the requested namespace: {label!r}. '
                        'Inspect it explicitly; no jobs were changed.'
                    )
                existing.append((jobid, label, command))
            planned = []
            if not remove:
                cursor.execute("SELECT COALESCE(current_setting('cron.timezone',true),'GMT')")
                zone = cursor.fetchone()[0]
                for definition in definitions:
                    if not definition['enabled']:
                        continue
                    if periodic_definition_status(definition['version'], definition['provider']) == 'reconciliation_required':
                        raise PgCronError(
                            f"Periodic definition {definition['name']!r} needs explicit SDK reconciliation first."
                        )
                    if definition['provider'] != 'pg_cron':
                        continue
                    planned.append((job_name(prefix, definition['name'], database),
                                    native_schedule(definition['cron_expr'], zone),
                                    occurrence_command(definition['name'], connection.connection)))
            if not dry_run:
                for jobid, _, _ in existing:
                    cursor.execute('SELECT cron.unschedule(%s)', [jobid])
                    if cursor.fetchone()[0] is not True:
                        raise PgCronError('A selected pg_cron job changed during reconciliation; transaction rolled back.')
                for label, schedule, command in planned:
                    cursor.execute('SELECT cron.schedule(%s,%s,%s)', [label, schedule, command])
            return {'removed': len(existing), 'scheduled': len(planned), 'dry_run': bool(dry_run),
                    'provider': 'pg_cron', 'occurrence_clock': 'UTC execution minute'}

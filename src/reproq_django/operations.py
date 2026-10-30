"""Explicit operator requests; Go owns task lifecycle state transitions."""

import os
import json
import subprocess
from datetime import timedelta
from collections.abc import Sequence
from types import SimpleNamespace

from django.db import connections

from .capabilities import require_worker_capabilities
from .db import parse_result_id
from .models import TaskRun
from .schema_contract import require_schema_contract
from .specs import read_task_spec
from .processes import capture_stdout


class OperationError(ValueError):
    pass


_TASK_COMMANDS = {'replay': ['replay'], 'retry': ['triage', 'retry'], 'cancel': ['cancel']}
_TASK_OPERATION_FIELDS = (
    'result_id', 'canonical_spec_text', 'spec_hash', 'spec_version', 'workflow_id',
    'task_path', 'backend_alias', 'queue_name', 'priority', 'lock_key',
    'concurrency_key', 'concurrency_limit', 'max_attempts', 'timeout_seconds',
)


def validate_task_operation_source(operation, row):
    """One source-intent gate for single requests and complete admin selections."""
    if operation not in _TASK_COMMANDS:
        raise OperationError('Unknown task operation')
    if operation == 'cancel':
        return None
    if row.spec_version != 1:
        raise OperationError('Legacy/unknown task specs cannot be replayed or retried; inspect original intent explicitly')
    if operation == 'retry' and row.workflow_id is not None:
        raise OperationError('Terminal workflow tasks cannot be retried in place; enqueue a fresh workflow or replay a detached single task')
    try:
        return read_task_spec(row)
    except ValueError as error:
        raise OperationError('Canonical task intent is invalid; inspect it before retry or replay') from error


def _selection_source_snapshot(queryset, identifiers, operation):
    sources = {}
    rows = queryset.filter(pk__in=identifiers).order_by('result_id').values(*_TASK_OPERATION_FIELDS).distinct()
    for fields in rows.iterator(chunk_size=1):
        row = SimpleNamespace(**fields)
        spec = validate_task_operation_source(operation, row)
        sources[row.result_id] = (spec.sha256, row.workflow_id)
    if set(sources) != set(identifiers):
        raise OperationError('Task selection changed during preflight; reload before requesting operations')
    return sources


def preflight_task_operations(queryset, operation):
    """Freeze at most100 IDs and inspect all source intent before any Go request.

    These reads do not promise an atomic batch: subsequent Go requests still
    revalidate individual tasks and report partial submission on runtime failure.
    No database lock or transaction is held across a worker request.
    """
    if operation not in _TASK_COMMANDS:
        raise OperationError('Unknown task operation')
    selected = queryset.order_by('result_id').values_list('result_id', flat=True).distinct()
    identifiers = tuple(selected[:101])
    if len(identifiers) > 100:
        raise OperationError('Select at most 100 tasks per operator request.')
    if identifiers and operation != 'cancel':
        sources = _selection_source_snapshot(queryset, identifiers, operation)
        if _selection_source_snapshot(queryset, identifiers, operation) != sources:
            raise OperationError('Task source changed during preflight; reload before requesting operations')
    if tuple(selected[:101]) != identifiers:
        raise OperationError('Task selection changed during preflight; reload before requesting operations')
    return identifiers


def _require_queue_schema(alias):
    try:
        require_schema_contract(connections[alias])
    except RuntimeError as error:
        raise OperationError('Selected queue schema requires migration/reconciliation before task operations') from error


def _operation_context(alias):
    from .management.commands.reproq import Command

    command = Command()
    binary, _, exists = command._resolve_worker_bin()
    if not exists:
        raise OperationError('Install the compatible Go worker before requesting task operations')
    capabilities = require_worker_capabilities(binary)
    dsn = command.get_dsn(alias)
    if not dsn:
        raise OperationError('Task operations require the selected PostgreSQL queue DSN')
    return binary, {**os.environ, 'REPROQ_DATABASE_URL': dsn}, capabilities


def _invoke(command, environment):
    try:
        return capture_stdout(command, env=environment, max_bytes=16384, timeout=30)
    except (subprocess.SubprocessError, OSError, ValueError) as error:
        # A lost commit acknowledgement does not prove no operation happened.
        # Never retry automatically or echo child credentials/error contents.
        raise OperationError('Task operation was not confirmed; inspect its current state before retrying') from error


def task_operation(operation, result_id, *, using=None):
    if operation not in _TASK_COMMANDS:
        raise OperationError('Unknown task operation')
    parsed_alias, raw_id = parse_result_id(result_id)
    alias = using or parsed_alias
    if using and ':' in str(result_id) and parsed_alias != using:
        raise OperationError('Result ID and explicit database alias disagree')
    try:
        identifier = int(raw_id)
    except (TypeError, ValueError) as error:
        raise OperationError('A positive task result ID is required') from error
    if identifier <= 0:
        raise OperationError('A positive task result ID is required')
    _require_queue_schema(alias)
    if operation in {'replay', 'retry'}:
        try:
            fields = TaskRun.objects.using(alias).values(*_TASK_OPERATION_FIELDS).get(pk=identifier)
        except TaskRun.DoesNotExist as error:
            raise OperationError('Task no longer exists; reload its current state') from error
        validate_task_operation_source(operation, SimpleNamespace(**fields))
    binary, environment, _ = _operation_context(alias)
    _invoke([binary, *_TASK_COMMANDS[operation], '--id', str(identifier)], environment)


def prune_history(
    *, using: str, statuses: Sequence[str], older_than: timedelta,
    field: str = 'finished_at', limit: int = 0, dry_run: bool = False,
) -> int:
    """Delegate retention to the canonical Go queue authority, without ORM deletes."""
    if not statuses or any(type(status) is not str or status not in {'SUCCESSFUL', 'FAILED', 'CANCELLED'} for status in statuses):
        raise OperationError('Prune accepts only terminal SUCCESSFUL, FAILED or CANCELLED history')
    if type(limit) is not int or limit < 0 or limit > 2**63 - 1:
        raise OperationError('Prune limit must be a nonnegative signed64 integer (0 = no limit)')
    if field not in {'finished_at', 'enqueued_at', 'updated_at', 'started_at'}:
        raise OperationError('Unsupported history retention timestamp field')
    if not isinstance(older_than, timedelta) or type(dry_run) is not bool:
        raise OperationError('History retention requires a timedelta age and boolean dry_run')
    microseconds = ((older_than.days * 86400 + older_than.seconds) * 1000000 + older_than.microseconds)
    if microseconds < 0 or microseconds > (2**63 - 1) // 1000:
        raise OperationError('History retention age is outside the Go duration range')
    _require_queue_schema(using)
    binary, environment, capabilities = _operation_context(using)
    supported = capabilities.get('operations')
    if not isinstance(supported, dict) or type(supported.get('history_prune')) is not int or supported['history_prune'] != 1:
        raise OperationError('Install a reviewed Go worker with history_prune operation version1')
    command = [binary, 'prune', 'history', '--statuses', ','.join(statuses),
               '--older-than', f'{microseconds}us', '--field', field, '--limit', str(limit), '--json']
    if dry_run:
        command.append('--dry-run')
    encoded = _invoke(command, environment)
    try:
        value = json.loads(encoded.decode('utf-8'))
        if (not isinstance(value, dict) or set(value) != {'count', 'dry_run'}
                or type(value['count']) is not int or not 0 <= value['count'] <= 2**63 - 1
                or (limit > 0 and value['count'] > limit)
                or type(value['dry_run']) is not bool or value['dry_run'] != dry_run):
            raise ValueError('Invalid history response')
    except (UnicodeError, ValueError, RecursionError) as error:
        raise OperationError('History cleanup was not confirmed; inspect queue state before retrying') from error
    return value['count']

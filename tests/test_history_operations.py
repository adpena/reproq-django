"""SDK operator boundary; PostgreSQL deletion/locking lives in Go's tests."""
from datetime import timedelta
from io import StringIO
import json
import subprocess
from unittest.mock import Mock

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db.models.query import QuerySet
import pytest

from reproq_django import operations


@pytest.fixture
def worker(monkeypatch):
    environment = {'DATABASE_URL': 'owned-business', 'REPROQ_DATABASE_URL': 'owned-queue'}
    context = Mock(return_value=('owned-worker', environment, {'operations': {'history_prune': 1}}))
    capture = Mock(return_value=b'{"count":7,"dry_run":false}\n')
    monkeypatch.setattr(operations, '_operation_context', context)
    monkeypatch.setattr(operations, '_require_queue_schema', Mock())
    monkeypatch.setattr(operations, 'capture_stdout', capture)
    monkeypatch.setattr(QuerySet, 'delete', lambda *a, **kw: pytest.fail('SDK used the ORM delete collector'))
    return context, capture, environment


def test_history_command_delegates_exact_intent_without_orm_or_dsn_argv(worker):
    context, capture, environment = worker
    capture.return_value = b'{"count":5,"dry_run":false}\n'
    output = StringIO()
    call_command('reproq', 'prune', statuses='FAILED,CANCELLED', older_than='30d',
                 field='updated_at', limit=5, stdout=output)
    context.assert_called_once_with('default')
    command = capture.call_args.args[0]
    assert command == ['owned-worker', 'prune', 'history', '--statuses', 'FAILED,CANCELLED',
                       '--older-than', '2592000000000us', '--field', 'updated_at',
                       '--limit', '5', '--json']
    assert capture.call_args.kwargs == {'env': environment, 'max_bytes': 16384, 'timeout': 30}
    assert 'owned-business' not in command and 'owned-queue' not in command
    assert 'Deleted 5 task(s).' in output.getvalue()


def test_history_dry_run_is_a_canonical_go_request(worker):
    _, capture, _ = worker
    capture.return_value = b'{"count":3,"dry_run":true}\n'
    output = StringIO()
    call_command('reproq', 'prune', dry_run=True, stdout=output)
    assert capture.call_args.args[0][-1] == '--dry-run'
    assert 'up to 3 terminal' in output.getvalue()


@pytest.mark.parametrize('statuses', ['', 'READY', 'RUNNING', 'WAITING', 'FAILED,FUTURE', 'SUCCESSFUL,READY'])
def test_active_or_unknown_history_intent_refuses_before_worker_request(worker, statuses):
    context, capture, _ = worker
    with pytest.raises(CommandError, match='terminal'):
        call_command('reproq', 'prune', statuses=statuses)
    context.assert_not_called()
    capture.assert_not_called()


@pytest.mark.parametrize('changes', [
    {'limit': -1}, {'limit': True}, {'limit': 2**63}, {'field': 'result_id'},
    {'older_than': timedelta(days=-1)}, {'older_than': timedelta(days=300*365)},
    {'older_than': '30d'}, {'dry_run': 1},
])
def test_invalid_retention_options_refuse_before_capability_or_queue_access(worker, changes):
    context, capture, _ = worker
    options = {'using': 'default', 'statuses': ['FAILED'], 'older_than': timedelta(days=30), **changes}
    with pytest.raises(operations.OperationError):
        operations.prune_history(**options)
    context.assert_not_called()
    capture.assert_not_called()


@pytest.mark.parametrize('capabilities', [
    {}, {'operations': None}, {'operations': {}},
    {'operations': {'history_prune': True}}, {'operations': {'history_prune': 2}},
])
def test_old_worker_cannot_receive_unknown_history_operation(worker, capabilities):
    context, capture, environment = worker
    context.return_value = ('owned-worker', environment, capabilities)
    with pytest.raises(operations.OperationError, match='history_prune'):
        operations.prune_history(using='default', statuses=['FAILED'], older_than=timedelta())
    capture.assert_not_called()


@pytest.mark.parametrize('payload', [
    None, [], {'count': True, 'dry_run': False}, {'count': -1, 'dry_run': False},
    {'count': 2.0, 'dry_run': False}, {'count': 2**63, 'dry_run': False},
    {'count': 2, 'dry_run': 0}, {'count': 2, 'dry_run': True},
    {'count': 2, 'dry_run': False, 'extra': 'unknown'},
])
def test_unconfirmed_history_response_never_triggers_automatic_retry(worker, payload):
    _, capture, _ = worker
    capture.return_value = json.dumps(payload).encode()
    with pytest.raises(operations.OperationError, match='not confirmed'):
        operations.prune_history(using='default', statuses=['FAILED'], older_than=timedelta())
    capture.assert_called_once()


def test_unencodable_response_and_timeout_have_the_same_unknown_commit_policy(worker):
    _, capture, _ = worker
    for outcome in (b'\xff', subprocess.TimeoutExpired(['owned-worker'], 30)):
        capture.reset_mock()
        capture.side_effect = outcome if isinstance(outcome, Exception) else None
        capture.return_value = outcome
        with pytest.raises(operations.OperationError, match='not confirmed'):
            operations.prune_history(using='default', statuses=['FAILED'], older_than=timedelta())
        capture.assert_called_once()


@pytest.mark.parametrize('dry_run', [False, True])
def test_worker_count_cannot_exceed_a_positive_requested_limit(worker, dry_run):
    _, capture, _ = worker
    capture.return_value = json.dumps({'count': 6, 'dry_run': dry_run}).encode()
    with pytest.raises(operations.OperationError, match='not confirmed'):
        operations.prune_history(using='default', statuses=['FAILED'], older_than=timedelta(),
                                 limit=5, dry_run=dry_run)
    capture.assert_called_once()


def test_retired_successful_command_is_not_a_second_retention_implementation(worker):
    context, capture, _ = worker
    with pytest.raises(CommandError, match='invalid choice'):
        call_command('reproq', 'prune-successful')
    context.assert_not_called()
    capture.assert_not_called()

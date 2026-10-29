"""Real admin/queryset preflight with an inert worker boundary and memory DB."""
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from django.conf import settings
from django.contrib.admin import AdminSite
from django.core.management import call_command
from django.db.models.query import QuerySet
from django.tasks.base import TASK_MAX_PRIORITY
from django.test import override_settings
import pytest

from reproq_django import operations
from reproq_django.models import TaskRun
from reproq_django.specs import build_spec, spec_fields


@pytest.fixture
def operator(monkeypatch):
    apps = list(settings.INSTALLED_APPS)
    apps.extend(name for name in ('django.contrib.sessions', 'django.contrib.messages',
                                 'django.contrib.admin') if name not in apps)
    with override_settings(INSTALLED_APPS=apps):
        from reproq_django.admin import TaskRunAdmin

        call_command('migrate', verbosity=0)
        TaskRun.objects.all().delete()
        admin = TaskRunAdmin(TaskRun, AdminSite())
        notices = []
        admin.message_user = lambda request, message, level=None: notices.append(str(message))
        context = Mock(return_value=('owned-worker', {'REPROQ_DATABASE_URL': 'owned-queue'}, {}))
        invoke = Mock(return_value=b'')
        monkeypatch.setattr(operations, '_operation_context', context)
        monkeypatch.setattr(operations, '_invoke', invoke)
        yield admin, SimpleNamespace(), notices, context, invoke
        monkeypatch.undo()
        TaskRun.objects.all().delete()


def failed(**changes):
    fields = {**spec_fields(build_spec(task_path='owned.admin.task')),
              'task_path': 'owned.admin.task', 'dedupe_active': False, 'status': 'FAILED'}
    return TaskRun.objects.create(**(fields | changes))


def reject_model_hydration_and_count(monkeypatch):
    monkeypatch.setattr(TaskRun, 'from_db', lambda *a, **kw: pytest.fail('Hydrated a TaskRun model'))
    monkeypatch.setattr(QuerySet, 'count', lambda *a, **kw: pytest.fail('Count-before-unbounded-selection'))


@pytest.mark.parametrize('operation', ['retry', 'replay', 'cancel'])
def test_complete_ordinary_selection_submits_without_models_or_count(operator, monkeypatch, operation):
    admin, request, notices, context, invoke = operator
    first, second = failed(), failed()
    reject_model_hydration_and_count(monkeypatch)
    admin._request_operation(request, TaskRun.objects.filter(status='FAILED'), operation)
    assert [call.args[0][-1] for call in invoke.call_args_list] == [str(first.pk), str(second.pk)]
    assert context.call_count == 2
    assert notices == [f'Submitted 2 {operation} request(s); inspect current task state.']


@pytest.mark.parametrize('operation,kind', [
    ('retry', 'legacy'), ('replay', 'legacy'), ('retry', 'workflow'),
    ('retry', 'checksum'), ('replay', 'checksum'),
    ('retry', 'delivery'), ('replay', 'delivery'),
    ('retry', 'native_options'), ('replay', 'native_options'),
])
def test_complete_mixed_selection_refuses_before_any_worker_request(operator, monkeypatch, operation, kind):
    admin, request, notices, context, invoke = operator
    first, second = failed(), failed()
    if kind == 'legacy':
        changes = {'spec_version': 0}
    elif kind == 'workflow':
        changes = {'workflow_id': uuid4()}
    elif kind == 'checksum':
        changes = {'canonical_spec_text': second.canonical_spec_text + ' '}
    elif kind == 'delivery':
        changes = {'queue_name': 'different-delivery'}
    else:
        # The shared stored-spec reader must reject invalid native options even
        # when the altered source is canonical and its checksum is truthful.
        import hashlib
        from reproq_django.serialization import canonical_json
        payload = dict(second.spec_json)
        payload['priority'] = TASK_MAX_PRIORITY + 1
        text = canonical_json(payload)
        changes = {'canonical_spec_text': text, 'spec_hash': hashlib.sha256(text.encode()).hexdigest(),
                   'priority': TASK_MAX_PRIORITY + 1}
    TaskRun.objects.filter(pk=second.pk).update(**changes)
    reject_model_hydration_and_count(monkeypatch)
    admin._request_operation(request, TaskRun.objects.order_by('result_id'), operation)
    context.assert_not_called()
    invoke.assert_not_called()
    assert list(TaskRun.objects.filter(pk__in=[first.pk, second.pk]).values_list('status', flat=True)) == ['FAILED', 'FAILED']
    assert notices[-1] == f'Submitted 0 {operation} request(s); inspect current task state.'
    assert len(notices) == 2


def test_cancel_allows_legacy_and_workflow_sources(operator, monkeypatch):
    admin, request, notices, _, invoke = operator
    failed(spec_version=0, canonical_spec_text=None)
    failed(workflow_id=uuid4(), canonical_spec_text='corrupt')
    monkeypatch.setattr(operations, 'validate_task_operation_source',
                        lambda *a: pytest.fail('Cancellation inspected canonical source'))
    admin.cancel_tasks(request, TaskRun.objects.all())
    assert invoke.call_count == 2 and notices[-1].startswith('Submitted 2 cancel')


def test_workflow_replay_remains_an_explicit_detached_request(operator):
    admin, request, notices, _, invoke = operator
    row = failed(workflow_id=uuid4())
    admin.replay_tasks(request, TaskRun.objects.filter(pk=row.pk))
    assert invoke.call_args.args[0] == ['owned-worker', 'replay', '--id', str(row.pk)]
    assert notices[-1].startswith('Submitted 1 replay')


def test_101_selected_rows_refuse_before_source_or_worker_access(operator, monkeypatch):
    admin, request, notices, context, invoke = operator
    for _ in range(101):
        failed()
    reject_model_hydration_and_count(monkeypatch)
    monkeypatch.setattr(operations, 'validate_task_operation_source',
                        lambda *a: pytest.fail('Over-limit selection inspected task source'))
    admin.retry_failed_tasks(request, TaskRun.objects.all())
    context.assert_not_called()
    invoke.assert_not_called()
    assert 'at most 100' in notices[0] and notices[-1].startswith('Submitted 0 retry')


def test_exactly_100_selected_rows_are_allowed(operator, monkeypatch):
    admin, request, notices, _, invoke = operator
    for _ in range(100):
        failed()
    reject_model_hydration_and_count(monkeypatch)
    admin.retry_failed_tasks(request, TaskRun.objects.all())
    assert invoke.call_count == 100 and notices[-1].startswith('Submitted 100 retry')


def test_all_source_validation_precedes_first_worker_request(operator, monkeypatch):
    admin, request, _, _, invoke = operator
    rows = [failed(), failed(), failed()]
    validated = set()
    original = operations.validate_task_operation_source
    def inspect(operation, row):
        result = original(operation, row)
        validated.add(row.result_id)
        return result
    monkeypatch.setattr(operations, 'validate_task_operation_source', inspect)
    def submit(command, environment):
        assert validated == {row.pk for row in rows}
    invoke.side_effect = submit
    admin.retry_failed_tasks(request, TaskRun.objects.all())
    assert invoke.call_count == 3


@pytest.mark.parametrize('change', ['disappears', 'membership', 'source', 'growth'])
def test_selection_changes_during_preflight_refuse_without_requests(operator, monkeypatch, change):
    admin, request, notices, context, invoke = operator
    first, second = failed(), failed()
    original = operations.validate_task_operation_source
    mutated = False
    def inspect(operation, row):
        nonlocal mutated
        result = original(operation, row)
        if not mutated:
            mutated = True
            if change == 'disappears':
                TaskRun.objects.filter(pk=second.pk).delete()
            elif change == 'membership':
                TaskRun.objects.filter(pk=second.pk).update(status='CANCELLED')
            elif change == 'source':
                TaskRun.objects.filter(pk=second.pk).update(**spec_fields(build_spec(task_path='owned.admin.task', args=[2])))
            else:
                failed()
        return result
    monkeypatch.setattr(operations, 'validate_task_operation_source', inspect)
    admin.retry_failed_tasks(request, TaskRun.objects.filter(pk__gte=first.pk))
    context.assert_not_called()
    invoke.assert_not_called()
    assert 'changed during preflight' in notices[0] and notices[-1].startswith('Submitted 0 retry')


def test_unexpected_runtime_failure_reports_only_confirmed_prior_submissions(operator):
    admin, request, notices, _, invoke = operator
    failed()
    failed()
    invoke.side_effect = [b'', operations.OperationError('Request not confirmed; inspect current state')]
    admin.retry_failed_tasks(request, TaskRun.objects.all())
    assert invoke.call_count == 2
    assert notices == ['Request not confirmed; inspect current state',
                       'Submitted 1 retry request(s); inspect current task state.']


def test_empty_selection_makes_no_worker_request(operator):
    admin, request, notices, context, invoke = operator
    admin.retry_failed_tasks(request, TaskRun.objects.none())
    context.assert_not_called()
    invoke.assert_not_called()
    assert notices == ['Submitted 0 retry request(s); inspect current task state.']


def test_large_canonical_sources_are_fetched_one_at_a_time(operator, monkeypatch):
    from django.db.backends.sqlite3.base import SQLiteCursorWrapper

    admin, request, notices, _, invoke = operator
    fields = spec_fields(build_spec(task_path='owned.admin.task', args=['x' * (128 * 1024) + '\U0001f600']))
    for _ in range(6):
        failed(**fields)
    batches = []
    original = SQLiteCursorWrapper.fetchmany
    def read(cursor, size=None):
        rows = original(cursor, size) if size is not None else original(cursor)
        if cursor.description and 'canonical_spec_text' in [column[0] for column in cursor.description]:
            batches.append(len(rows))
        return rows
    monkeypatch.setattr(SQLiteCursorWrapper, 'fetchmany', read)
    admin.retry_failed_tasks(request, TaskRun.objects.all())
    assert batches and max(batches) == 1
    assert invoke.call_count == 6 and notices[-1].startswith('Submitted 6 retry')

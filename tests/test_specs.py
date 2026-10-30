from datetime import datetime

import pytest

from reproq_django.specs import build_spec, read_spec


@pytest.mark.parametrize('changes', [
    {'priority': 1.5}, {'priority': True}, {'priority': 2**31}, {'priority': -(2**31)-1},
    {'max_attempts': 1.9}, {'max_attempts': True}, {'max_attempts': 0}, {'max_attempts': '3'},
    {'timeout_seconds': 0}, {'timeout_seconds': False}, {'timeout_seconds': 3.2},
    {'concurrency_limit': -1}, {'concurrency_limit': True}, {'concurrency_limit': 1.5},
    {'concurrency_key': {}}, {'concurrency_key': ''}, {'lock_key': ''}, {'lock_key': 1},
    {'backend_alias': ''}, {'queue_name': '\x00'}, {'task_path': ' '},
    {'run_after': datetime(2026, 1, 1)}, {'run_after': 'tomorrow'},
    {'args': 'abc'}, {'kwargs': False}, {'kwargs': []}, {'kwargs': {1: 'not-a-keyword'}},
    {'takes_context': 1}, {'code_ref': False},
])
def test_invalid_spec_fields_fail_without_lossy_coercion(changes):
    with pytest.raises(ValueError):
        build_spec(**{'task_path': 'owned.task', **changes})


def test_stored_spec_reader_preserves_encoded_arguments_and_known_options():
    from datetime import timedelta

    original = build_spec(task_path='owned.task', args=(timedelta(seconds=17),),
                          timeout_seconds=42, lock_key='operator-mutex')
    inspected = read_spec(original.text, original.sha256, original.version)
    assert inspected == original
    assert inspected.payload['exec']['timeout_seconds'] == 42
    assert inspected.payload['lock_key'] == 'operator-mutex'


def test_stored_spec_reader_rejects_unknown_intent_or_noncanonical_bytes():
    import hashlib
    import json

    original = build_spec(task_path='owned.task')
    for text, version in [(original.text, 99), (json.dumps(original.payload, indent=2), 1)]:
        with pytest.raises(ValueError):
            read_spec(text, hashlib.sha256(text.encode()).hexdigest(), version)
    payload = {**original.payload, 'unknown_future_option': True}
    text = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    with pytest.raises(ValueError):
        read_spec(text, hashlib.sha256(text.encode()).hexdigest(), 1)
    with pytest.raises(ValueError, match='checksum'):
        read_spec(original.text, '0' * 64, 1)


def test_priority_boundaries_use_the_native_django_task_authority():
    from django.tasks.base import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY
    for priority in (TASK_MIN_PRIORITY,TASK_MAX_PRIORITY):
        original=build_spec(task_path='owned.task',priority=priority)
        assert read_spec(original.text,original.sha256,1)==original


def test_native_priority_outside_bounds_is_rejected_by_build_and_stored_reader():
    import hashlib
    from django.tasks.base import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY
    from reproq_django.serialization import canonical_json
    for priority in (TASK_MIN_PRIORITY-1,TASK_MAX_PRIORITY+1):
        with pytest.raises(ValueError,match='priority'):
            build_spec(task_path='owned.task',priority=priority)
        payload={**build_spec(task_path='owned.task').payload,'priority':priority}
        text=canonical_json(payload)
        with pytest.raises(ValueError,match='priority'):
            read_spec(text,hashlib.sha256(text.encode()).hexdigest(),1)

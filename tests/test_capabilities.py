import json
import os
import subprocess
import sys

import pytest

from reproq_django.capabilities import executor_capabilities, require_worker_capabilities


def test_executor_probe_requires_neither_settings_nor_database():
    environment = {key: value for key, value in os.environ.items()
                   if key in {'PATH', 'LANG', 'LC_ALL', 'TMPDIR', 'SYSTEMROOT', 'WINDIR'}}
    environment['DJANGO_SETTINGS_MODULE'] = 'intentionally_missing_settings'
    process = subprocess.run([sys.executable, '-m', 'reproq_django.executor', '--capabilities'],
                             env=environment, capture_output=True, timeout=30)
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout) == executor_capabilities()
    assert process.stderr == b''


@pytest.mark.parametrize('response', [
    b'old worker: unknown command', b'[]', b'{}',
    b'{"worker_version":"old","schema_id":"reproq-schema-v1","schema_version":1,"executor_protocol":1}',
    b'{"worker_version":"bad","schema_id":"reproq-schema-v2","schema_version":true,"executor_protocol":2}',
])
def test_incompatible_worker_refused_without_echoing_child_output(monkeypatch, response):
    monkeypatch.setattr('reproq_django.capabilities._capture_worker_capabilities', lambda binary: response)
    with pytest.raises(ValueError, match='incompatible') as error:
        require_worker_capabilities('owned-worker')
    assert response.decode() not in str(error.value)


def test_matching_worker_contract_is_accepted(monkeypatch):
    value = {**executor_capabilities(), 'worker_version': 'owned'}
    monkeypatch.setattr('reproq_django.capabilities._capture_worker_capabilities', lambda binary: json.dumps(value).encode())
    assert require_worker_capabilities('owned-worker') == value

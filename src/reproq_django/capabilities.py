"""Provider compatibility, checked before installation or queue claims."""

import json
import subprocess

from . import __version__
from .processes import capture_stdout
from .schema_contract import SCHEMA_CONTRACT_ID, SCHEMA_VERSION

EXECUTOR_PROTOCOL = 2
_CAPABILITY_MAX_BYTES = 16384
_CAPABILITY_TIMEOUT_SECONDS = 30


def _capture_worker_capabilities(binary):
    return capture_stdout(
        [str(binary), 'capabilities'], max_bytes=_CAPABILITY_MAX_BYTES,
        timeout=_CAPABILITY_TIMEOUT_SECONDS,
    )


def executor_capabilities():
    return {'sdk_version': __version__, 'schema_id': SCHEMA_CONTRACT_ID,
            'schema_version': SCHEMA_VERSION, 'executor_protocol': EXECUTOR_PROTOCOL}


def require_worker_capabilities(binary):
    """Only call after digest verification when installing a downloaded file."""
    try:
        output = _capture_worker_capabilities(binary)
        value = json.loads(output.decode('utf-8'))
        if not isinstance(value, dict):
            raise ValueError('Expected a capability object')
        expected = executor_capabilities()
        for key in ('schema_id', 'schema_version', 'executor_protocol'):
            if type(value.get(key)) is not type(expected[key]) or value[key] != expected[key]:
                raise ValueError('Incompatible provider contract')
        if not isinstance(value.get('worker_version'), str) or not value['worker_version']:
            raise ValueError('Worker version missing')
        operations = value.get('operations')
        if (not isinstance(operations, dict)
                or type(operations.get('worker_python_bin')) is not int
                or operations['worker_python_bin'] != 1):
            raise ValueError('Worker lacks explicit executor Python selection')
        return value
    except (OSError, subprocess.SubprocessError, ValueError, RecursionError) as error:
        # Do not include child output or command/environment in the diagnostic.
        raise ValueError(
            'Worker is incompatible with this SDK. Install a reviewed worker '
            f'with schema {SCHEMA_VERSION}, executor protocol {EXECUTOR_PROTOCOL}, '
            'and worker_python_bin operation version1; '
            'no queue runtime was started and no task was claimed.'
        ) from error

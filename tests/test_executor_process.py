"""Real provider processes, with a migrated owned SQLite database and real pipes."""

import json
import os
from pathlib import Path
import subprocess
import sys
import shlex

import pytest


@pytest.fixture(scope="module")
def provider(tmp_path_factory):
    root = tmp_path_factory.mktemp("owned-executor")
    (root / "owned_settings.py").write_text(
        "import os\nSECRET_KEY='owned-test-only'\n"
        "DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': os.environ['OWNED_EXECUTOR_DATABASE']}}\n"
        "INSTALLED_APPS=['django.contrib.contenttypes','reproq_django','owned_app.OwnedConfig']\n"
        "TASKS={'default':{'BACKEND':'reproq_django.backend.ReproqBackend'}}\n"
        "USE_TZ=True\n"
    )
    (root / "owned_app.py").write_text(
        "from django.apps import AppConfig\n"
        "class OwnedConfig(AppConfig):\n"
        " name='owned_app'\n"
        " def ready(self):\n"
        "  import os\n"
        "  print('SETUP_LOG')\n"
        "  os.write(1,b'NATIVE_SETUP_LOG\\n')\n"
        "  from reproq_django.signals import task_started,task_finished\n"
        "  def log(**kwargs): print('HOOK_LOG')\n"
        "  task_started.connect(log,weak=False)\n"
        "  task_finished.connect(log,weak=False)\n"
    )
    (root / "owned_tasks.py").write_text(
        "import os\nprint('IMPORT_LOG')\nos.write(1,b'NATIVE_IMPORT_LOG\\n')\n"
        "def task(context, mode):\n"
        " assert context.task_result.raw_status=='RUNNING'\n"
        " assert context.task_result.args==[mode]\n"
        " assert context.task_result.backend=='default'\n"
        " print('TASK_LOG')\n"
        " os.write(1,b'NATIVE_TASK_LOG\\n')\n"
        " if mode=='large_logs':\n"
        "  import tracemalloc\n"
        "  tracemalloc.start()\n"
        "  chunk='x'*8192\n"
        "  for _ in range(1024): print(chunk)\n"
        "  peak=tracemalloc.get_traced_memory()[1]\n"
        "  tracemalloc.stop()\n"
        "  return {'peak':peak}\n"
        " if mode=='oversized': return 'x'*(4*1024*1024)\n"
        " if mode=='invalid': return float('nan')\n"
        " if mode=='stale':\n"
        "  from uuid import uuid4\n"
        "  from reproq_django.models import TaskRun\n"
        "  TaskRun.objects.filter(pk=context.result_id).update(lease_token=uuid4())\n"
        " if mode=='failure': raise RuntimeError('x'*(4*1024*1024))\n"
        " return {'zero':0,'false':False,'unicode':'Texas 😀'}\n"
        "async def async_task(context, mode):\n"
        " result=await context.task_result.task.aget_result(context.task_result.id)\n"
        " assert result.task.func is async_task and result.raw_status=='RUNNING'\n"
        " return task(context,mode)\n"
    )
    env = {key: os.environ[key] for key in
           ("PATH", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT", "WINDIR", "COMSPEC")
           if key in os.environ}
    env.update(PYTHONPATH=os.pathsep.join([str(root), str(Path(__file__).resolve().parents[1] / "src")]),
               DJANGO_SETTINGS_MODULE="owned_settings", OWNED_EXECUTOR_DATABASE=str(root / "queue.sqlite3"),
               PYTHONNOUSERSITE="1", PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-c", "import django; django.setup(); from django.core.management import call_command; call_command('migrate',verbosity=0)"],
        env=env, capture_output=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return root, env


@pytest.mark.parametrize("mode", ["normal", "async", "large_logs", "oversized", "invalid", "stale", "failure"])
def test_real_executor_has_one_complete_bounded_result_and_separate_streaming_logs(provider, mode):
    root, env = provider
    payload = {"task_path": "owned_tasks.async_task" if mode == "async" else "owned_tasks.task",
               "takes_context": True, "args": [mode], "kwargs": {}, "queue_name": "default"}
    metadata_path = root / (mode + "-claim.json")
    prepare = (
        "import django; django.setup(); from reproq_django.models import TaskRun; "
        "from reproq_django.specs import build_spec,spec_fields; "
        "from django.utils import timezone; from datetime import timedelta; from uuid import uuid4; "
        "import json; from pathlib import Path; "
        f"payload=json.loads({json.dumps(payload)!r}); "
        "token=uuid4(); spec=build_spec(**payload); row=TaskRun.objects.create(**spec_fields(spec),task_path=payload['task_path'],"
        "status='RUNNING',attempts=1,leased_by='owned-provider',lease_token=token,"
        "worker_ids=['owned-provider'],"
        "leased_until=timezone.now()+timedelta(minutes=2)); "
        f"Path({str(metadata_path)!r}).write_text(json.dumps({{'id':row.pk,'token':str(token)}}))"
    )
    prepared = subprocess.run([sys.executable, "-c", prepare], env=env, capture_output=True, timeout=30)
    assert prepared.returncode == 0, prepared.stderr
    claim = json.loads(metadata_path.read_text())
    command = [sys.executable, "-m", "reproq_django.executor", "--payload-stdin",
               "--result-id", str(claim["id"]), "--attempt", "1", "--worker-id", "owned-provider",
               "--lease-token", claim["token"], "--max-output-bytes", "1024", "--max-payload-bytes", "1024"]
    log_path = root / (mode + ".log")
    with log_path.open("wb") as logs:
        result = subprocess.run(command, input=json.dumps(payload).encode(), env=env,
                                stdout=subprocess.PIPE, stderr=logs, timeout=30)
    assert len(result.stdout) <= 1024
    envelope = json.loads(result.stdout)
    assert result.stdout.count(b"\n") == 1
    remaining = {b"SETUP_LOG", b"IMPORT_LOG", b"TASK_LOG", b"HOOK_LOG",
                 b"NATIVE_SETUP_LOG", b"NATIVE_IMPORT_LOG", b"NATIVE_TASK_LOG"}
    with log_path.open("rb") as log:
        tail = b""
        while block := log.read(64 * 1024):
            content = tail + block
            remaining = {marker for marker in remaining if marker not in content}
            tail = content[-32:]
    assert not remaining
    if mode in {"normal", "async", "large_logs"}:
        assert result.returncode == 0 and envelope["ok"] is True
        if mode == "large_logs":
            assert envelope["return"]["peak"] < 1024 * 1024
            assert log_path.stat().st_size > 8 * 1024 * 1024
        else:
            assert envelope["return"] == {"zero": 0, "false": False, "unicode": "Texas 😀"}
    else:
        assert result.returncode != 0 and envelope["ok"] is False
        assert envelope["exception_class"] == {
            "oversized": "reproq_django.protocol.ProtocolLimitError", "invalid": "builtins.ValueError",
            "stale": "reproq_django.context.LeaseLostError", "failure": "builtins.RuntimeError",
        }[mode]


@pytest.mark.skipif(os.name == "nt", reason="POSIX foreground process replacement; native Windows qualification is separate")
def test_foreground_management_replaces_parent_and_preserves_child_exit_status(provider):
    root, base_env = provider
    worker = root / "owned-worker"
    identity = root / "foreground-pid.txt"
    capabilities = json.dumps({'worker_version': 'owned', 'schema_id': 'reproq-schema-v2',
                               'schema_version': 2, 'executor_protocol': 2})
    business_dsn = 'postgresql://owned@127.0.0.1/business'
    queue_dsn = 'postgresql://owned@127.0.0.1/queue'
    worker.write_text('#!/bin/sh\nif [ "$1" = capabilities ]; then\n'
                      + "printf '%s\\n' " + shlex.quote(capabilities) + '\nexit 0\nfi\n'
                      + '[ "$DATABASE_URL" = ' + shlex.quote(business_dsn) + ' ] || exit 9\n'
                      + '[ "$REPROQ_DATABASE_URL" = ' + shlex.quote(queue_dsn) + ' ] || exit 10\n'
                      + "printf '%s\\n' \"$$\" > " + shlex.quote(str(identity)) + "\nexit 7\n")
    worker.chmod(0o755)
    environment = {**base_env, "REPROQ_WORKER_BIN": str(worker),
                   "DATABASE_URL": business_dsn}
    source = (
        "import django,sys; django.setup(); "
        "from django.core.management import call_command; "
        "from reproq_django.management.commands.reproq import Command; "
        "Command.get_dsn=lambda self,alias=None:'postgresql://owned@127.0.0.1/queue'; "
        "sys.argv=['manage.py','reproq','worker','--allowed-task-modules','owned_tasks']; "
        "call_command('reproq','worker','--allowed-task-modules','owned_tasks')"
    )
    process = subprocess.Popen([sys.executable, "-c", source], cwd=root, env=environment,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate(timeout=30)
    assert process.returncode == 7, (
        f"stdout={stdout!r}\nstderr={stderr.decode('utf-8', errors='backslashreplace')}"
    )
    assert int(identity.read_text()) == process.pid

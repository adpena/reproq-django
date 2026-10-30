"""Exercise the packaged Linux supervisor with harmless child processes."""
from __future__ import annotations

from importlib.resources import files
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import subprocess
import sys
import time

import pytest


def _supported_bash():
    if os.name != "posix":
        return None
    binary = shutil.which("bash")
    if binary is None:
        return None
    result = subprocess.run(
        [binary, "-c", 'printf "%s.%s" "${BASH_VERSINFO[0]}" "${BASH_VERSINFO[1]}"'],
        capture_output=True, text=True, timeout=5, check=True,
    )
    version = tuple(int(part) for part in result.stdout.split("."))
    return binary if version >= (4, 2) else None


BASH = _supported_bash()
pytestmark = pytest.mark.skipif(
    os.name != "posix" or BASH is None,
    reason="The packaged Render supervisor requires Bash 4.2+; native Linux proof is separate.",
)


PROBE = '''\
import json, os, signal, sys, time
from pathlib import Path

arguments = sys.argv[1:]
if arguments[0] == "reproq":
    operation = arguments[1]
    role = "prestart" if operation == "check" else operation
else:
    role = arguments[0]
event_file = Path(os.environ["REPROQ_TEST_EVENTS"])
def stop(signum, frame):
    with event_file.open("a") as stream:
        stream.write(json.dumps({"event": "stop", "role": role, "signal": signum}) + "\\n")
    raise SystemExit(128 + signum)
signal.signal(signal.SIGTERM, stop)
descriptor = os.open(event_file, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
try:
    os.write(descriptor, (json.dumps({"event": "start", "role": role, "arguments": arguments,
        "pid": os.getpid(), "ppid": os.getppid(), "interpreter": sys.executable,
        "process_role": os.environ.get("REPROQ_PROCESS_ROLE")}) + "\\n").encode())
finally:
    os.close(descriptor)
if role == "prestart":
    raise SystemExit(0)
if role == "worker" and os.environ.get("REPROQ_TEST_RESTART"):
    starts = [json.loads(line) for line in event_file.read_text().splitlines()]
    if sum(row["event"] == "start" and row["role"] == "worker" for row in starts) == 1:
        raise SystemExit(23)
if role == "web":
    required = {"worker"}
    if os.environ["REPROQ_TEST_EXPECT_BEAT"]:
        required.add(os.environ["REPROQ_TEST_EXPECT_BEAT"])
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        rows = [json.loads(line) for line in event_file.read_text().splitlines()]
        started = [row for row in rows if row["event"] == "start"]
        worker_count = sum(row["role"] == "worker" for row in started)
        if required <= {row["role"] for row in started} and worker_count >= (2 if os.environ.get("REPROQ_TEST_RESTART") else 1):
            if os.environ.get("REPROQ_TEST_ENDING") == "term":
                Path(os.environ["REPROQ_TEST_READY"]).touch()
                break
            raise SystemExit(int(os.environ.get("REPROQ_TEST_WEB_EXIT", "0")))
        time.sleep(0.01)
    else:
        raise SystemExit("Expected child processes did not start")
while True:
    signal.pause()
'''


def run_supervisor(tmp_path, *, mode, low_memory, expected_beat, beat_command=None,
                   active_environment=True, overrides=None, uv_fails=False,
                   ending="exit", restart=False, web_exit=0, expected_error=None):
    """Use the real resource; replace only external application commands."""
    probe = tmp_path / "probe.py"
    probe.write_text(PROBE)
    binary_directory = tmp_path / "tools"
    binary_directory.mkdir()
    selected_environment = tmp_path / "selected environment with spaces"
    selected_python = selected_environment / "bin" / "python"
    selected_python.parent.mkdir(parents=True)
    selected_python.symlink_to(sys.executable)
    (tmp_path / "manage.py").write_text(PROBE)
    python = shlex.quote(sys.executable)
    command = f"{python} {shlex.quote(str(probe))}"
    uv = binary_directory / "uv"
    uv_calls = tmp_path / "uv-calls"
    uv.write_text(
        f'#!/bin/sh\nprintf "resolve\\n" >> {shlex.quote(str(uv_calls))}\n'
        + ("exit 17\n" if uv_fails else
           f'test "$1" = run && test "$2" = python || exit 19\nshift 2\nexec {shlex.quote(str(selected_python))} "$@"\n')
    )
    uv.chmod(0o700)
    # Noninteractive login Bash may reset PATH. BASH_ENV then supplies only the
    # owned fake uv executable; no HOME or global environment is changed.
    bash_environment = tmp_path / "bash-env"
    bash_environment.write_text(f'export PATH={shlex.quote(str(binary_directory))}:"$PATH"\n')
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith("REPROQ_") and key not in {"LOW_MEMORY_MODE", "BASH_ENV", "VIRTUAL_ENV"}
    }
    events = tmp_path / "events.jsonl"
    environment.update({
        "PATH": str(binary_directory) + os.pathsep + environment.get("PATH", ""),
        "BASH_ENV": str(bash_environment), "LOW_MEMORY_MODE": low_memory,
        "REPROQ_TEST_EVENTS": str(events), "REPROQ_TEST_EXPECT_BEAT": expected_beat,
        "REPROQ_WEB_CMD": f"exec {command} web",
        "REPROQ_PRESTART_MAX_WAIT_SECONDS": "1", "REPROQ_RESTART_DELAY_SECONDS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "REPROQ_TEST_ENDING": ending, "REPROQ_TEST_WEB_EXIT": str(web_exit),
        "REPROQ_TEST_READY": str(tmp_path / "web-ready"),
    })
    if active_environment:
        environment["VIRTUAL_ENV"] = str(selected_environment if active_environment is True else tmp_path / "broken")
    if restart:
        environment["REPROQ_TEST_RESTART"] = "1"
    if mode is not None:
        environment["REPROQ_SCHEDULER_MODE"] = mode
    if beat_command is not None:
        environment["REPROQ_BEAT_CMD"] = (
            f"exec {command} custom-beat" if beat_command == "custom" else beat_command
        )
    if overrides:
        environment.update({key: value.replace("@probe", command) for key, value in overrides.items()})
    resource = Path(str(files("reproq_django").joinpath("resources", "render_start.sh")))
    process = subprocess.Popen(
        [BASH, str(resource)], cwd=tmp_path, env=environment, text=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
    )
    try:
        if ending == "term":
            deadline = time.monotonic() + 5
            while not (tmp_path / "web-ready").exists():
                if process.poll() is not None or time.monotonic() > deadline:
                    pytest.fail("The supervisor did not start all roles before TERM")
                time.sleep(0.01)
            process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate(timeout=3)
        pytest.fail(f"Supervisor did not exit with its web process: {stdout}\n{stderr}")
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)
    calls = uv_calls.read_text().splitlines() if uv_calls.exists() else []
    if expected_error is not None:
        assert process.returncode == 1 and expected_error in stdout, (stdout, stderr)
        assert not events.exists()
        return {"uv_calls": calls}
    assert process.returncode == (143 if ending == "term" else web_exit), (stdout, stderr)
    all_rows = [json.loads(line) for line in events.read_text().splitlines()]
    rows = [row for row in all_rows if row["event"] == "start"]
    roles = {row["role"] for row in rows}
    assert roles == {"prestart", "worker", "web", *([expected_beat] if expected_beat else [])}, (rows, stdout, stderr)
    assert sum(row["role"] == expected_beat for row in rows) == bool(expected_beat)
    assert sum(row["role"] == "worker" for row in rows) == (2 if restart else 1)
    assert next(row for row in rows if row["role"] == "worker")["process_role"] == "worker"
    if expected_beat:
        assert next(row for row in rows if row["role"] == expected_beat)["process_role"] == "beat"
    else:
        assert "reproq-beat starting" not in stdout
    assert "LOW_MEMORY_MODE enabled; disabling reproq-beat" not in stdout
    return {"rows": rows, "events": all_rows, "uv_calls": calls,
            "selected_python": str(selected_python), "stdout": stdout}


@pytest.mark.parametrize("mode", ["beat", "off", "pg_cron"])
@pytest.mark.parametrize("low_memory", ["0", "1"])
def test_explicit_scheduler_mode_is_independent_of_low_memory(tmp_path, mode, low_memory):
    run_supervisor(tmp_path, mode=mode, low_memory=low_memory,
                   expected_beat="beat" if mode == "beat" else "")


@pytest.mark.parametrize("low_memory", ["0", "1"])
def test_unset_mode_retains_default_beat_command(tmp_path, low_memory):
    run_supervisor(tmp_path, mode=None, low_memory=low_memory, expected_beat="beat")


@pytest.mark.parametrize("low_memory", ["0", "1"])
def test_custom_beat_command_retains_selected_process_role(tmp_path, low_memory):
    run_supervisor(tmp_path, mode="beat", low_memory=low_memory,
                   expected_beat="custom-beat", beat_command="custom")


@pytest.mark.parametrize("mode", ["off", "pg_cron"])
def test_nonbeat_mode_never_runs_custom_beat_command(tmp_path, mode):
    run_supervisor(tmp_path, mode=mode, low_memory="1", expected_beat="", beat_command="custom")


@pytest.mark.parametrize("disabled", ["", "0", "false", "off", "disabled", "none"])
def test_explicit_disabled_beat_command_is_preserved(tmp_path, disabled):
    run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="", beat_command=disabled)


def test_existing_scheduler_mode_normalization_is_preserved(tmp_path):
    run_supervisor(tmp_path, mode="  BeAt  ", low_memory="1", expected_beat="beat")


@pytest.mark.parametrize("active_environment", [True, False])
@pytest.mark.parametrize("mode", ["beat", "off", "pg_cron"])
def test_default_commands_reuse_one_selected_interpreter_with_spaces(tmp_path, active_environment, mode):
    result = run_supervisor(tmp_path, mode=mode, low_memory="1",
        expected_beat="beat" if mode == "beat" else "", active_environment=active_environment)
    assert len(result["uv_calls"]) == (0 if active_environment else 1)
    defaults = [row for row in result["rows"] if row["role"] != "web"]
    assert all(row["interpreter"] == result["selected_python"] for row in defaults)
    worker = next(row for row in defaults if row["role"] == "worker")
    assert worker["arguments"] == ["reproq", "worker", "--concurrency", "3",
                                   "--metrics-addr", "127.0.0.1:9090"]
    if mode == "beat":
        assert next(row for row in defaults if row["role"] == "beat")["arguments"] == [
            "reproq", "beat", "--interval", "30s"]


def test_explicit_empty_worker_and_prestart_retain_defaults(tmp_path):
    result = run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="beat",
        active_environment=False, overrides={"REPROQ_WORKER_CMD": "", "REPROQ_PRESTART_CMD": ""})
    assert result["uv_calls"] == ["resolve"]


@pytest.mark.parametrize("beat_command", ["custom", ""])
@pytest.mark.parametrize("active_environment", [False, "broken"])
def test_custom_commands_require_neither_an_active_environment_nor_uv(tmp_path, beat_command, active_environment):
    custom = "exec @probe worker 'literal spaces' 'semi;colon' 'dollar$(false)'"
    result = run_supervisor(tmp_path, mode="beat", low_memory="1",
        expected_beat="custom-beat" if beat_command else "", beat_command=beat_command,
        active_environment=active_environment, uv_fails=True,
        overrides={"REPROQ_PRESTART_CMD": "exec @probe prestart", "REPROQ_WORKER_CMD": custom})
    assert not result["uv_calls"]
    assert next(row for row in result["rows"] if row["role"] == "worker")["arguments"] == [
        "worker", "literal spaces", "semi;colon", "dollar$(false)"]


@pytest.mark.parametrize("mode", ["off", "pg_cron"])
def test_custom_commands_do_not_resolve_a_disabled_default_beat(tmp_path, mode):
    result = run_supervisor(tmp_path, mode=mode, low_memory="1", expected_beat="",
        active_environment=False, uv_fails=True,
        overrides={"REPROQ_PRESTART_CMD": "exec @probe prestart",
                   "REPROQ_WORKER_CMD": "exec @probe worker"})
    assert not result["uv_calls"]


def test_default_runtime_keeps_configured_worker_and_beat_flags(tmp_path):
    result = run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="beat",
        overrides={"REPROQ_CONCURRENCY": "7", "METRICS_ADDR": "[::1]:9191", "REPROQ_BEAT_INTERVAL": "2m"})
    assert next(row for row in result["rows"] if row["role"] == "worker")["arguments"] == [
        "reproq", "worker", "--concurrency", "7", "--metrics-addr", "[::1]:9191"]
    assert next(row for row in result["rows"] if row["role"] == "beat")["arguments"] == [
        "reproq", "beat", "--interval", "2m"]


def test_broken_active_environment_never_resolves_a_different_runtime(tmp_path):
    result = run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="beat",
        active_environment="broken", expected_error="executable absolute path")
    assert not result["uv_calls"]


def test_failed_runtime_resolution_starts_no_roles(tmp_path):
    result = run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="beat",
        active_environment=False, uv_fails=True, expected_error="could not resolve")
    assert result["uv_calls"] == ["resolve"]


@pytest.mark.parametrize("ending", ["exit", "term"])
def test_selected_runtime_retains_restart_exit_and_shutdown(tmp_path, ending):
    result = run_supervisor(tmp_path, mode="beat", low_memory="1", expected_beat="beat",
        active_environment=False, ending=ending, restart=True, web_exit=42)
    assert result["uv_calls"] == ["resolve"]
    stopped = {row["role"] for row in result["events"] if row["event"] == "stop"}
    assert stopped == ({"worker", "beat", "web"} if ending == "term" else {"worker", "beat"})

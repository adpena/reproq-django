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
if arguments[0] == "uv":
    operation = arguments[arguments.index("reproq") + 1]
    role = "prestart" if operation == "check" else operation
else:
    role = arguments[0]
event_file = Path(os.environ["REPROQ_TEST_EVENTS"])
descriptor = os.open(event_file, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
try:
    os.write(descriptor, (json.dumps({"role": role, "arguments": arguments,
        "process_role": os.environ.get("REPROQ_PROCESS_ROLE")}) + "\\n").encode())
finally:
    os.close(descriptor)
if role == "prestart":
    raise SystemExit(0)
if role == "web":
    required = {"worker"}
    if os.environ["REPROQ_TEST_EXPECT_BEAT"]:
        required.add(os.environ["REPROQ_TEST_EXPECT_BEAT"])
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        rows = [json.loads(line) for line in event_file.read_text().splitlines()]
        if required <= {row["role"] for row in rows}:
            raise SystemExit(0)
        time.sleep(0.01)
    raise SystemExit("Expected child processes did not start")
while True:
    signal.pause()
'''


def run_supervisor(tmp_path, *, mode, low_memory, expected_beat, beat_command=None):
    """Use the real resource; replace only external application commands."""
    probe = tmp_path / "probe.py"
    probe.write_text(PROBE)
    binary_directory = tmp_path / "bin"
    binary_directory.mkdir()
    python = shlex.quote(sys.executable)
    command = f"{python} {shlex.quote(str(probe))}"
    uv = binary_directory / "uv"
    uv.write_text(f'#!/bin/sh\nexec {command} uv "$@"\n')
    uv.chmod(0o700)
    # Noninteractive login Bash may reset PATH. BASH_ENV then supplies only the
    # owned fake uv executable; no HOME or global environment is changed.
    bash_environment = tmp_path / "bash-env"
    bash_environment.write_text(f'export PATH={shlex.quote(str(binary_directory))}:"$PATH"\n')
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith("REPROQ_") and key not in {"LOW_MEMORY_MODE", "BASH_ENV"}
    }
    events = tmp_path / "events.jsonl"
    environment.update({
        "BASH_ENV": str(bash_environment), "LOW_MEMORY_MODE": low_memory,
        "REPROQ_TEST_EVENTS": str(events), "REPROQ_TEST_EXPECT_BEAT": expected_beat,
        "REPROQ_WEB_CMD": f"exec {command} web",
        "REPROQ_PRESTART_MAX_WAIT_SECONDS": "1", "REPROQ_RESTART_DELAY_SECONDS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
    })
    if mode is not None:
        environment["REPROQ_SCHEDULER_MODE"] = mode
    if beat_command is not None:
        environment["REPROQ_BEAT_CMD"] = (
            f"exec {command} custom-beat" if beat_command == "custom" else beat_command
        )
    resource = Path(str(files("reproq_django").joinpath("resources", "render_start.sh")))
    process = subprocess.Popen(
        [BASH, str(resource)], env=environment, text=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=8)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate(timeout=3)
        pytest.fail(f"Supervisor did not exit with its web process: {stdout}\n{stderr}")
    assert process.returncode == 0, (stdout, stderr)
    rows = [json.loads(line) for line in events.read_text().splitlines()]
    roles = {row["role"] for row in rows}
    assert roles == {"prestart", "worker", "web", *([expected_beat] if expected_beat else [])}, (rows, stdout, stderr)
    assert sum(row["role"] == expected_beat for row in rows) == bool(expected_beat)
    assert sum(row["role"] == "worker" for row in rows) == 1
    assert next(row for row in rows if row["role"] == "worker")["process_role"] == "worker"
    if expected_beat:
        assert next(row for row in rows if row["role"] == expected_beat)["process_role"] == "beat"
    else:
        assert "reproq-beat starting" not in stdout
    assert "LOW_MEMORY_MODE enabled; disabling reproq-beat" not in stdout


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

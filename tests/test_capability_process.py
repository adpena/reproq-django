"""Actual bounded control-channel processes; no queue or Django setup in child."""

import json
import os
from pathlib import Path
import sys
import time
import tracemalloc

import pytest

from reproq_django.capabilities import (
    executor_capabilities,
    require_worker_capabilities,
)


pytestmark = pytest.mark.skipif(
    os.name != "posix",
    reason="Actual POSIX process-group proof; native Windows qualification is separate",
)


def helper(tmp_path, mode):
    binary = tmp_path / "owned-capability-worker"
    payload = json.dumps({**executor_capabilities(), "worker_version": "owned", "operations": {"worker_python_bin": 1}})
    binary.write_text(
        "#!" + sys.executable + "\n"
        "import json,os,subprocess,sys,time\n"
        f"mode={mode!r}\n"
        f"pid_file={str(tmp_path / 'child.pid')!r}\n"
        'if mode=="overflow":\n'
        ' for _ in range(4096): os.write(1,b"x"*8192)\n'
        'elif mode=="stderr":\n'
        ' for _ in range(1024): os.write(2,b"x"*8192)\n'
        'elif mode=="hang": time.sleep(30)\n'
        'elif mode=="descendant":\n'
        ' child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(30)"])\n'
        ' open(pid_file,"w").write(str(child.pid))\n'
        f"print({payload!r},flush=True)\n"
        'if mode=="nonzero": sys.exit(7)\n'
    )
    binary.chmod(0o755)
    return binary


@pytest.mark.parametrize("mode", ["normal", "stderr"])
def test_actual_valid_probe_discards_stderr_and_has_bounded_parent_allocations(
    tmp_path, mode
):
    binary = helper(tmp_path, mode)
    tracemalloc.start()
    try:
        result = require_worker_capabilities(binary)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert result["worker_version"] == "owned" and peak < 2 * 1024 * 1024


def test_excessive_output_is_stopped_while_reading_with_bounded_parent_memory(tmp_path):
    binary = helper(tmp_path, "overflow")
    started = time.monotonic()
    tracemalloc.start()
    try:
        with pytest.raises(ValueError, match="incompatible"):
            require_worker_capabilities(binary)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 2 * 1024 * 1024 and time.monotonic() - started < 5


@pytest.mark.parametrize("mode", ["hang", "descendant"])
def test_probe_timeout_cleans_up_inherited_pipe_descendants(
    tmp_path, monkeypatch, mode
):
    binary = helper(tmp_path, mode)
    monkeypatch.setattr("reproq_django.capabilities._CAPABILITY_TIMEOUT_SECONDS", 0.5)
    started = time.monotonic()
    with pytest.raises(ValueError, match="incompatible"):
        require_worker_capabilities(binary)
    assert time.monotonic() - started < 5
    child = tmp_path / "child.pid"
    if child.exists():
        identifier = int(child.read_text())
        deadline = time.monotonic() + 3
        while True:
            try:
                os.kill(identifier, 0)
            except ProcessLookupError:
                break
            # Linux may briefly retain an already-dead orphan as a zombie.
            stat = Path(f"/proc/{identifier}/stat")
            if stat.exists() and stat.read_text().split()[2] == "Z":
                break
            assert time.monotonic() < deadline, "Probe descendant survived termination"
            time.sleep(0.02)


def test_nonzero_probe_rejects_an_otherwise_valid_contract(tmp_path):
    with pytest.raises(ValueError, match="incompatible"):
        require_worker_capabilities(helper(tmp_path, "nonzero"))

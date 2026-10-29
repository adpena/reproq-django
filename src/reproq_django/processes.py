"""Bounded internal worker command execution; no shell or captured task logs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import os
from pathlib import Path
import signal
import subprocess
import threading
import time


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        # Native Windows qualification is separate. Terminate descendants of
        # a still-running command as well as its direct process.
        try:
            subprocess.run(
                [str(Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32" / "taskkill.exe"),
                 "/F", "/T", "/PID", str(process.pid)],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, timeout=5, check=False,
            )
        except (OSError, subprocess.SubprocessError):
            pass
        if process.poll() is None:
            process.kill()
    process.wait(timeout=5)


def capture_stdout(
    command: Sequence[str], *, max_bytes: int, timeout: float,
    env: Mapping[str, str] | None = None,
) -> bytes:
    """Capture at most the byte budget plus one overflow byte, with a deadline."""
    if max_bytes <= 0 or timeout <= 0:
        raise ValueError("A positive command output budget and timeout are required")
    process = subprocess.Popen(
        command, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, bufsize=0, start_new_session=os.name == "posix",
    )
    stdout = process.stdout
    assert stdout is not None
    output = bytearray()
    complete = threading.Event()
    errors: list[OSError] = []

    def read():
        try:
            while len(output) <= max_bytes:
                block = os.read(stdout.fileno(), min(4096, max_bytes + 1 - len(output)))
                if not block:
                    break
                output.extend(block)
        except OSError as error:
            errors.append(error)
        finally:
            complete.set()

    reader = threading.Thread(target=read, name="reproq-command-reader", daemon=True)
    deadline = time.monotonic() + timeout
    reader.start()
    try:
        if not complete.wait(timeout):
            raise subprocess.TimeoutExpired(command, timeout)
        if errors:
            raise errors[0]
        if len(output) > max_bytes:
            raise ValueError("Worker command exceeded its output byte budget")
        status = process.wait(timeout=max(0, deadline - time.monotonic()))
        if status:
            raise subprocess.CalledProcessError(status, command)
        return bytes(output)
    finally:
        _stop_process(process)
        reader.join(timeout=5)
        stdout.close()
        if reader.is_alive():
            raise ValueError("Command reader did not stop after process termination")

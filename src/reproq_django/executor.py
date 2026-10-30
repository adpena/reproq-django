"""One-shot Python task provider, invoked by a fenced Go worker claim."""

import argparse
import asyncio
import inspect
import io
import json
import os
import signal
import sys
from uuid import UUID

from .protocol import bounded_traceback, encode_envelope, failure_envelope, read_payload


class ExecutorTerminated(RuntimeError):
    pass


def setup_django(settings_module: str | None = None):
    if settings_module:
        os.environ["DJANGO_SETTINGS_MODULE"] = settings_module
    import django

    django.setup()


def _diagnostic(error):
    # Share the protocol formatter: no source-file reads, custom exception
    # formatting or unbounded source lines on the separately drained log pipe.
    print("[reproq executor]", bounded_traceback(error, 16384), file=sys.stderr)


def _run_task(args, spec):
    setup_django(args.settings)

    from reproq_django.context import ExecutionClaim, TaskContext
    from reproq_django.db import resolve_queue_db
    from reproq_django.signals import task_finished, task_started

    task_path = spec.get("task_path")
    if not isinstance(task_path, str) or not task_path:
        raise ValueError("The task payload must contain a task_path")
    if args.task_path is not None and args.task_path != task_path:
        raise ValueError("Task path override disagrees with the canonical payload")

    context = TaskContext.from_claim(
        ExecutionClaim(
            result_id=args.result_id,
            budget_attempt=args.attempt,
            expected_task_path=task_path,
            db_alias=resolve_queue_db(spec.get("queue_name")),
            worker_id=args.worker_id,
            lease_token=str(args.lease_token),
        )
    )
    source_task = context.task_result.task
    real_callable = source_task.func

    def emit_signal(signal_obj, **payload):
        for _, response in signal_obj.send_robust(
            sender="reproq_django.executor", **payload
        ):
            if isinstance(response, Exception):
                _diagnostic(response)

    try:
        # The row was loaded under the current claim fence. Its decoded
        # arguments are cached for native sync/async result/context access.
        task_args, task_kwargs = (
            context.task_result.decoded_args,
            context.task_result.decoded_kwargs,
        )
        emit_signal(task_started, task_context=context)
        if source_task.takes_context:
            result = real_callable(context, *task_args, **task_kwargs)
        else:
            result = real_callable(*task_args, **task_kwargs)
        if inspect.isawaitable(result):

            async def await_result():
                return await result

            result = asyncio.run(await_result())
        encoded = encode_envelope({"ok": True, "return": result}, args.max_output_bytes)
        context.save_metadata()
        emit_signal(task_finished, task_context=context, ok=True)
        return encoded
    except Exception as error:
        emit_signal(
            task_finished, task_context=context, ok=False, error=type(error).__name__
        )
        raise


def execute():
    if sys.argv[1:] == ["--capabilities"]:
        from .capabilities import executor_capabilities

        # No settings imports, Django setup or database access during this probe.
        print(
            json.dumps(executor_capabilities(), sort_keys=True, separators=(",", ":"))
        )
        return
    # Reserve the protocol pipe before setup/imports/hooks/task code can print.
    # Native writes and child processes inherit stderr as their log stream.
    sys.stdout.flush()
    sys.stderr.flush()
    protocol = os.fdopen(os.dup(sys.stdout.fileno()), "wb", buffering=0)
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())

    parser = argparse.ArgumentParser(description="Reproq Django Task Executor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--payload-stdin", action="store_true")
    group.add_argument("--payload-file")
    group.add_argument("--payload-json")
    parser.add_argument("--task-path")
    parser.add_argument("--settings")
    parser.add_argument("--result-id", required=True)
    parser.add_argument("--attempt", type=int, required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--lease-token", type=UUID, required=True)
    parser.add_argument("--max-output-bytes", type=int, required=True)
    parser.add_argument("--max-payload-bytes", type=int, required=True)
    args = parser.parse_args()
    if args.max_output_bytes < len(b'{"ok":false}\n') or args.max_payload_bytes <= 0:
        parser.error(
            "Worker byte limits are too small for a complete protocol envelope"
        )
    if args.attempt <= 0 or not args.worker_id.strip():
        parser.error("A positive attempt and nonempty worker owner are required")

    def terminated(sig, frame):
        raise ExecutorTerminated(f"Task terminated by signal {sig}")

    signal.signal(signal.SIGTERM, terminated)
    signal.signal(signal.SIGINT, terminated)
    exit_code = 0
    try:
        if args.payload_stdin:
            spec = read_payload(sys.stdin.buffer, args.max_payload_bytes)
        elif args.payload_file:
            with open(args.payload_file, "rb") as stream:
                spec = read_payload(stream, args.max_payload_bytes)
        else:
            spec = read_payload(
                io.BytesIO(args.payload_json.encode("utf-8")), args.max_payload_bytes
            )
        encoded = _run_task(args, spec)
    except Exception as error:
        _diagnostic(error)
        encoded = failure_envelope(error, args.max_output_bytes)
        exit_code = 1
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
    # Never publish a partial success prefix if encoding fails.
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = protocol.write(remaining)
            if not written:
                raise OSError(
                    "Protocol stream closed before the complete result was written"
                )
            remaining = remaining[written:]
    finally:
        protocol.close()
    raise SystemExit(exit_code)


if __name__ == "__main__":
    execute()

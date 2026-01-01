import argparse
import asyncio
import contextlib
import inspect
import io
import json
import os
import signal
import sys
import traceback

def setup_django(settings_module: str = None):
    if settings_module:
        os.environ["DJANGO_SETTINGS_MODULE"] = settings_module
    import django
    django.setup()

def execute():
    parser = argparse.ArgumentParser(description="Reproq Django Task Executor")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--payload-stdin", action="store_true")
    group.add_argument("--payload-file", type=str)
    group.add_argument("--payload-json", type=str)

    parser.add_argument("--task-path", type=str)
    parser.add_argument("--settings", type=str)
    parser.add_argument("--result-id", type=str)
    parser.add_argument("--attempt", type=int, default=1)

    args = parser.parse_args()

    def emit_result(payload, exit_code=None):
        print(json.dumps(payload), file=sys.stdout)
        sys.stdout.flush()
        if exit_code is not None:
            sys.exit(exit_code)

    def debug_log(message):
        if os.environ.get("REPROQ_EXECUTOR_DEBUG"):
            print(f"[reproq executor] {message}", file=sys.stderr)

    # Load payload
    try:
        if args.payload_stdin:
            payload_raw = sys.stdin.read()
        elif args.payload_file:
            with open(args.payload_file, "r") as f:
                payload_raw = f.read()
        else:
            payload_raw = args.payload_json
        
        spec = json.loads(payload_raw)
    except Exception as e:
        debug_log(f"Failed to parse payload: {e}")
        emit_result(
            {
                "ok": False,
                "exception_class": "PayloadError",
                "message": f"Failed to parse payload: {str(e)}",
            },
            exit_code=1,
        )

    setup_django(args.settings or spec.get("django", {}).get("settings_module"))

    from django.utils.module_loading import import_string
    from reproq_django.context import TaskContext
    from reproq_django.db import resolve_queue_db
    from reproq_django.models import TaskRun
    from reproq_django.serialization import decode_args_kwargs, DeserializationError
    from reproq_django.signals import task_finished, task_started
    
    task_path = args.task_path or spec.get("task_path")
    try:
        callable_task = import_string(task_path)
    except Exception as e:
        debug_log(f"Failed to import task {task_path}: {e}")
        emit_result(
            {
                "ok": False,
                "exception_class": "ImportError",
                "message": f"Failed to import task {task_path}: {str(e)}",
            },
            exit_code=1,
        )

    if hasattr(callable_task, "func"):
        real_callable = callable_task.func
    elif hasattr(callable_task, "run"):
        real_callable = callable_task.run
    else:
        real_callable = callable_task

    queue_name = spec.get("queue_name")
    db_alias = resolve_queue_db(queue_name)
    try:
        existing_metadata = TaskRun.objects.using(db_alias).values_list(
            "metadata_json", flat=True
        ).get(result_id=args.result_id)
        if existing_metadata is None:
            existing_metadata = {}
    except TaskRun.DoesNotExist:
        existing_metadata = {}
    except Exception as exc:
        debug_log(f"Failed to load metadata: {exc}")
        existing_metadata = {}

    context = TaskContext(
        result_id=args.result_id,
        attempt=args.attempt,
        task_path=task_path,
        queue_name=queue_name,
        priority=spec.get("priority"),
        db_alias=db_alias,
        metadata=existing_metadata,
    )

    # Signal handling
    def signal_handler(sig, frame):
        debug_log(f"Task terminated by signal {sig}")
        emit_result(
            {
                "ok": False,
                "exception_class": "Terminated",
                "message": f"Task terminated by signal {sig}",
            },
            exit_code=1,
        )
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    def _emit_signal(signal_obj, **payload):
        try:
            signal_obj.send(sender="reproq_django.executor", **payload)
        except Exception as exc:
            debug_log(f"Signal failed: {exc}")

    try:
        raw_args = spec.get("args", [])
        raw_kwargs = spec.get("kwargs", {})
        try:
            task_args, task_kwargs = decode_args_kwargs(raw_args, raw_kwargs, using=db_alias)
        except DeserializationError as exc:
            raise RuntimeError(f"Failed to deserialize arguments: {exc}") from exc
        debug_log(f"Executing task {task_path} (result_id={args.result_id}, attempt={args.attempt})")

        stdout_capture = io.StringIO()
        with contextlib.redirect_stdout(stdout_capture):
            if spec.get("takes_context") or getattr(callable_task, "takes_context", False):
                _emit_signal(task_started, task_context=context)
                result_val = real_callable(context, *task_args, **task_kwargs)
            else:
                _emit_signal(task_started, task_context=context)
                result_val = real_callable(*task_args, **task_kwargs)

            # Support for async tasks
            if inspect.iscoroutine(result_val):
                result_val = asyncio.run(result_val)

        captured_stdout = stdout_capture.getvalue()
        if captured_stdout:
            print(
                f"[reproq executor] Task wrote {len(captured_stdout)} bytes to stdout; suppressed.",
                file=sys.stderr,
            )
            debug_log(f"Captured stdout:\n{captured_stdout}")
        
        # Verify serializability
        try:
            json.dumps(result_val)
        except TypeError:
            raise TypeError(f"Return value of type {type(result_val)} is not JSON serializable")

        try:
            context.save_metadata()
        except Exception as exc:
            debug_log(f"Failed to save metadata: {exc}")

        _emit_signal(task_finished, task_context=context, ok=True)
        emit_result({"ok": True, "return": result_val})

    except Exception as e:
        debug_log(f"Task execution failed: {e}")
        try:
            context.save_metadata()
        except Exception as exc:
            debug_log(f"Failed to save metadata: {exc}")
        _emit_signal(task_finished, task_context=context, ok=False, error=str(e))
        emit_result(
            {
                "ok": False,
                "exception_class": e.__class__.__name__,
                "message": str(e),
                "traceback": traceback.format_exc(),
            },
            exit_code=1,
        )

if __name__ == "__main__":
    execute()

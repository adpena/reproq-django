import argparse
import json
import sys
import os
import traceback
import signal
import asyncio
import inspect
from typing import Any

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
        print(json.dumps({
            "ok": False,
            "exception_class": "PayloadError",
            "message": f"Failed to parse payload: {str(e)}"
        }), file=sys.stdout)
        sys.exit(1)

    setup_django(args.settings or spec.get("django", {}).get("settings_module"))

    from django.utils.module_loading import import_string
    
    task_path = args.task_path or spec.get("task_path")
    try:
        callable_task = import_string(task_path)
    except Exception as e:
        print(json.dumps({
            "ok": False,
            "exception_class": "ImportError",
            "message": f"Failed to import task {task_path}: {str(e)}"
        }), file=sys.stdout)
        sys.exit(1)

    # Context for task
    context = {
        "result_id": args.result_id,
        "attempt": args.attempt,
        "spec_hash": None, # Could be passed in CLI if needed
        "task_path": task_path,
        "queue_name": spec.get("queue_name"),
        "priority": spec.get("priority"),
    }

    # Signal handling
    def signal_handler(sig, frame):
        print(json.dumps({
            "ok": False,
            "exception_class": "Terminated",
            "message": f"Task terminated by signal {sig}"
        }), file=sys.stdout)
        sys.exit(1)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        task_args = spec.get("args", [])
        task_kwargs = spec.get("kwargs", {})
        
        if spec.get("takes_context") or getattr(callable_task, "takes_context", False):
            result_val = callable_task(context, *task_args, **task_kwargs)
        else:
            result_val = callable_task(*task_args, **task_kwargs)
        
        # Support for async tasks
        if inspect.iscoroutine(result_val):
            result_val = asyncio.run(result_val)
        
        # Verify serializability
        try:
            json.dumps(result_val)
        except TypeError:
            raise TypeError(f"Return value of type {type(result_val)} is not JSON serializable")

        print(json.dumps({"ok": True, "return": result_val}), file=sys.stdout)

    except Exception as e:
        print(json.dumps({
            "ok": False,
            "exception_class": e.__class__.__name__,
            "message": str(e),
            "traceback": traceback.format_exc()
        }), file=sys.stdout)
        sys.exit(1)

if __name__ == "__main__":
    execute()

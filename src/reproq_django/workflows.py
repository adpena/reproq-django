import uuid
from typing import List, Any, Tuple
from django.utils import timezone
from .proxy import TaskResultProxy
from .models import TaskRun, WorkflowRun
from .serialization import encode_args_kwargs
from .db import resolve_queue_db

class Chain:
    """
    A simple workflow to execute tasks sequentially.
    """
    def __init__(self, *tasks):
        self.tasks = tasks # List of (task, args, kwargs) or just task
        self.workflow_id = uuid.uuid4()

    def enqueue(self) -> List[TaskResultProxy]:
        last_id = None
        results = []
        db_alias = None
        
        for i, item in enumerate(self.tasks):
            if isinstance(item, tuple):
                task, args, kwargs = item
            else:
                task, args, kwargs = item, (), {}
            
            # The first task is READY, subsequent are WAITING
            status = "READY" if i == 0 else "WAITING"
            wait_count = 0 if i == 0 else 1
            
            # We bypass the backend's enqueue to handle the complex state
            backend = task.get_backend()
            queue_name = task.queue_name or "default"
            alias = resolve_queue_db(queue_name)
            if db_alias is None:
                db_alias = alias
            elif alias != db_alias:
                raise ValueError("All workflow tasks must target the same database")
            
            # Normalize spec (reusing logic from backend would be better)
            # For brevity, we create the record directly
            encoded_args, encoded_kwargs = encode_args_kwargs(args, kwargs)
            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": queue_name,
                "priority": task.priority or 0,
                "lock_key": getattr(task, "lock_key", None),
                "concurrency_key": getattr(task, "concurrency_key", None),
                "concurrency_limit": getattr(task, "concurrency_limit", 0) or 0,
                "exec": {
                    "timeout_seconds": int(getattr(backend, "options", {}).get("TIMEOUT_SECONDS", 900)),
                    "max_attempts": int(getattr(backend, "options", {}).get("MAX_ATTEMPTS", 3)),
                },
            }
            run = TaskRun.objects.using(db_alias).create(
                backend_alias=backend.alias,
                queue_name=queue_name,
                priority=task.priority or 0,
                spec_json=spec,
                task_path=task.module_path,
                spec_hash=uuid.uuid4().hex, # Workflows bypass simple dedupe for now
                status=status,
                parent_id=last_id,
                workflow_id=self.workflow_id,
                wait_count=wait_count,
                errors_json=[],
                max_attempts=spec["exec"]["max_attempts"],
                timeout_seconds=spec["exec"]["timeout_seconds"],
                lock_key=spec["lock_key"],
                concurrency_key=spec["concurrency_key"],
                concurrency_limit=spec["concurrency_limit"],
            )
            
            last_id = run.result_id
            results.append(backend._result_proxy(run.result_id, db_alias=db_alias))
            
        return results

class Group:
    """
    A workflow to execute tasks in parallel.
    """
    def __init__(self, *tasks):
        self.tasks = tasks
        self.workflow_id = uuid.uuid4()

    def enqueue(self) -> List[TaskResultProxy]:
        results = []
        db_alias = None
        for item in self.tasks:
            if isinstance(item, tuple):
                task, args, kwargs = item
            else:
                task, args, kwargs = item, (), {}
            
            backend = task.get_backend()
            queue_name = task.queue_name or "default"
            alias = resolve_queue_db(queue_name)
            if db_alias is None:
                db_alias = alias
            elif alias != db_alias:
                raise ValueError("All workflow tasks must target the same database")
            
            encoded_args, encoded_kwargs = encode_args_kwargs(args, kwargs)
            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": queue_name,
                "priority": task.priority or 0,
                "lock_key": getattr(task, "lock_key", None),
                "concurrency_key": getattr(task, "concurrency_key", None),
                "concurrency_limit": getattr(task, "concurrency_limit", 0) or 0,
                "exec": {
                    "timeout_seconds": int(getattr(backend, "options", {}).get("TIMEOUT_SECONDS", 900)),
                    "max_attempts": int(getattr(backend, "options", {}).get("MAX_ATTEMPTS", 3)),
                },
            }
            run = TaskRun.objects.using(db_alias).create(
                backend_alias=backend.alias,
                queue_name=queue_name,
                priority=task.priority or 0,
                spec_json=spec,
                task_path=task.module_path,
                spec_hash=uuid.uuid4().hex,
                status="READY",
                workflow_id=self.workflow_id,
                errors_json=[],
                max_attempts=spec["exec"]["max_attempts"],
                timeout_seconds=spec["exec"]["timeout_seconds"],
                lock_key=spec["lock_key"],
                concurrency_key=spec["concurrency_key"],
                concurrency_limit=spec["concurrency_limit"],
            )
            results.append(backend._result_proxy(run.result_id, db_alias=db_alias))
            
        return results

class Chord:
    """
    A workflow to execute tasks in parallel and then run a callback once all complete.
    """
    def __init__(self, tasks: Tuple[Any, ...], callback):
        self.tasks = tasks
        self.callback = callback
        self.workflow_id = uuid.uuid4()

    def enqueue(self) -> Tuple[List[TaskResultProxy], TaskResultProxy]:
        results = []
        db_alias = None

        for item in self.tasks:
            if isinstance(item, tuple):
                task, args, kwargs = item
            else:
                task, args, kwargs = item, (), {}

            backend = task.get_backend()
            queue_name = task.queue_name or "default"
            alias = resolve_queue_db(queue_name)
            if db_alias is None:
                db_alias = alias
            elif alias != db_alias:
                raise ValueError("All workflow tasks must target the same database")

            encoded_args, encoded_kwargs = encode_args_kwargs(args, kwargs)
            spec = {
                "v": 1,
                "task_path": task.module_path,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
                "takes_context": getattr(task, "takes_context", False),
                "queue_name": queue_name,
                "priority": task.priority or 0,
                "lock_key": getattr(task, "lock_key", None),
                "concurrency_key": getattr(task, "concurrency_key", None),
                "concurrency_limit": getattr(task, "concurrency_limit", 0) or 0,
                "exec": {
                    "timeout_seconds": int(getattr(backend, "options", {}).get("TIMEOUT_SECONDS", 900)),
                    "max_attempts": int(getattr(backend, "options", {}).get("MAX_ATTEMPTS", 3)),
                },
            }
            run = TaskRun.objects.using(db_alias).create(
                backend_alias=backend.alias,
                queue_name=queue_name,
                priority=task.priority or 0,
                spec_json=spec,
                task_path=task.module_path,
                spec_hash=uuid.uuid4().hex,
                status="READY",
                workflow_id=self.workflow_id,
                wait_count=0,
                errors_json=[],
                max_attempts=spec["exec"]["max_attempts"],
                timeout_seconds=spec["exec"]["timeout_seconds"],
                lock_key=spec["lock_key"],
                concurrency_key=spec["concurrency_key"],
                concurrency_limit=spec["concurrency_limit"],
            )
            results.append(backend._result_proxy(run.result_id, db_alias=db_alias))

        callback_task = self.callback
        if isinstance(callback_task, tuple):
            cb_task, cb_args, cb_kwargs = callback_task
        else:
            cb_task, cb_args, cb_kwargs = callback_task, (), {}

        cb_backend = cb_task.get_backend()
        cb_queue = cb_task.queue_name or "default"
        cb_alias = resolve_queue_db(cb_queue)
        if db_alias is None:
            db_alias = cb_alias
        elif cb_alias != db_alias:
            raise ValueError("All workflow tasks must target the same database")
        wait_count = len(results)
        cb_status = "READY" if wait_count == 0 else "WAITING"
        workflow_status = "WAITING_CALLBACK" if wait_count == 0 else "RUNNING"

        cb_encoded_args, cb_encoded_kwargs = encode_args_kwargs(cb_args, cb_kwargs)
        cb_spec = {
            "v": 1,
            "task_path": cb_task.module_path,
            "args": cb_encoded_args,
            "kwargs": cb_encoded_kwargs,
            "takes_context": getattr(cb_task, "takes_context", False),
            "queue_name": cb_queue,
            "priority": cb_task.priority or 0,
            "lock_key": getattr(cb_task, "lock_key", None),
            "concurrency_key": getattr(cb_task, "concurrency_key", None),
            "concurrency_limit": getattr(cb_task, "concurrency_limit", 0) or 0,
            "exec": {
                "timeout_seconds": int(getattr(cb_backend, "options", {}).get("TIMEOUT_SECONDS", 900)),
                "max_attempts": int(getattr(cb_backend, "options", {}).get("MAX_ATTEMPTS", 3)),
            },
        }
        cb_run = TaskRun.objects.using(db_alias).create(
            backend_alias=cb_backend.alias,
            queue_name=cb_queue,
            priority=cb_task.priority or 0,
            spec_json=cb_spec,
            task_path=cb_task.module_path,
            spec_hash=uuid.uuid4().hex,
            status=cb_status,
            workflow_id=self.workflow_id,
            wait_count=wait_count,
            errors_json=[],
            max_attempts=cb_spec["exec"]["max_attempts"],
            timeout_seconds=cb_spec["exec"]["timeout_seconds"],
            lock_key=cb_spec["lock_key"],
            concurrency_key=cb_spec["concurrency_key"],
            concurrency_limit=cb_spec["concurrency_limit"],
        )

        WorkflowRun.objects.using(db_alias).create(
            workflow_id=self.workflow_id,
            expected_count=wait_count,
            success_count=0,
            failure_count=0,
            callback_result_id=cb_run.result_id,
            status=workflow_status,
            created_at=timezone.now(),
            updated_at=timezone.now(),
        )

        return results, cb_backend._result_proxy(cb_run.result_id, db_alias=db_alias)

def chain(*tasks) -> Chain:
    return Chain(*tasks)

def group(*tasks) -> Group:
    return Group(*tasks)

def chord(*tasks, callback) -> Chord:
    return Chord(tasks, callback)

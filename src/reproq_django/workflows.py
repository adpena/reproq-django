import uuid
from typing import List, Any
from django.tasks import Task
from .proxy import TaskResultProxy
from .models import TaskRun

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
        
        for i, item in enumerate(self.tasks):
            if isinstance(item, tuple):
                task, args, kwargs = item
            else:
                task, args, kwargs = item, (), {}
            
            # The first task is READY, subsequent are WAITING
            status = "READY" if i == 0 else "WAITING"
            wait_count = 0 if i == 0 else 1
            
            # We bypass the backend's enqueue to handle the complex state
            from django.tasks import tasks
            backend = tasks[task.backend or "default"]
            
            # Normalize spec (reusing logic from backend would be better)
            # For brevity, we create the record directly
            run = TaskRun.objects.create(
                backend_alias=backend.alias,
                queue_name=task.queue_name or "default",
                priority=task.priority or 0,
                spec_json={
                    "task_path": task.module_path,
                    "args": args,
                    "kwargs": kwargs,
                    "v": 1
                },
                spec_hash=uuid.uuid4().hex, # Workflows bypass simple dedupe for now
                status=status,
                parent_id=last_id,
                workflow_id=self.workflow_id,
                wait_count=wait_count,
                errors_json=[]
            )
            
            last_id = run.result_id
            results.append(TaskResultProxy(str(run.result_id), backend))
            
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
        from django.tasks import tasks
        
        for item in self.tasks:
            if isinstance(item, tuple):
                task, args, kwargs = item
            else:
                task, args, kwargs = item, (), {}
            
            backend = tasks[task.backend or "default"]
            
            run = TaskRun.objects.create(
                backend_alias=backend.alias,
                queue_name=task.queue_name or "default",
                priority=task.priority or 0,
                spec_json={
                    "task_path": task.module_path,
                    "args": args,
                    "kwargs": kwargs,
                    "v": 1
                },
                spec_hash=uuid.uuid4().hex,
                status="READY",
                workflow_id=self.workflow_id,
                errors_json=[]
            )
            results.append(TaskResultProxy(str(run.result_id), backend))
            
        return results

def chain(*tasks) -> Chain:
    return Chain(*tasks)

def group(*tasks) -> Group:
    return Group(*tasks)

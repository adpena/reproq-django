from django.http import JsonResponse
from django.db.models import Count
from .models import TaskRun, Worker, PeriodicTask

def reproq_stats_api(request):
    """
    Returns JSON statistics about the Reproq queue and workers.
    """
    if not request.user.is_staff:
        return JsonResponse({"error": "Unauthorized"}, status=403)

    task_stats = TaskRun.objects.values("status").annotate(count=Count("result_id"))
    worker_stats = Worker.objects.all().values("worker_id", "hostname", "concurrency", "queues", "last_seen_at", "version")
    periodic_stats = PeriodicTask.objects.all().values("name", "cron_expr", "enabled", "next_run_at")

    return JsonResponse({
        "tasks": {s["status"]: s["count"] for s in task_stats},
        "workers": list(worker_stats),
        "periodic": list(periodic_stats),
    })

def reproq_stress_test_api(request):
    """
    Triggers a stress test via API.
    """
    if not request.user.is_staff or request.method != "POST":
        return JsonResponse({"error": "Unauthorized"}, status=403)

    from .tasks import debug_noop_task
    count = int(request.POST.get("count", 100))
    
    for _ in range(count):
        debug_noop_task.enqueue(sleep_seconds=0.1)

    return JsonResponse({"ok": True, "enqueued": count})

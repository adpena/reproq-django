import hmac
import os

from django.conf import settings
from django.db.models import Count
from django.http import JsonResponse

from .models import TaskRun, Worker, PeriodicTask

AUTH_HEADER = "Authorization"
TOKEN_HEADER = "X-Reproq-Token"


def _get_stats_token():
    token = getattr(settings, "REPROQ_STATS_TOKEN", "") or os.environ.get("REPROQ_STATS_TOKEN", "")
    if token:
        return token
    return os.environ.get("METRICS_AUTH_TOKEN", "")


def _token_from_request(request):
    auth = request.headers.get(AUTH_HEADER, "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return request.headers.get(TOKEN_HEADER, "")


def _authorized(request):
    token = _get_stats_token()
    if token:
        candidate = _token_from_request(request)
        if candidate and hmac.compare_digest(candidate, token):
            return True
    return request.user.is_authenticated and request.user.is_staff

def reproq_stats_api(request):
    """
    Returns JSON statistics about the Reproq queue and workers.
    """
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)

    task_stats = TaskRun.objects.values("status").annotate(count=Count("result_id"))
    queue_stats = TaskRun.objects.values("queue_name", "status").annotate(count=Count("result_id"))
    worker_stats = Worker.objects.all().values("worker_id", "hostname", "concurrency", "queues", "last_seen_at", "version")
    periodic_stats = PeriodicTask.objects.all().values("name", "cron_expr", "enabled", "next_run_at")

    queues = {}
    for row in queue_stats:
        queue = row["queue_name"] or "default"
        status = row["status"]
        queues.setdefault(queue, {})[status] = row["count"]

    return JsonResponse({
        "tasks": {s["status"]: s["count"] for s in task_stats},
        "queues": queues,
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

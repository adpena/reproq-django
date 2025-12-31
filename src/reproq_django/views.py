import hmac
import os
import urllib.error
import urllib.request

from django.conf import settings
from django.db.models import Count
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET

from .models import TaskRun, Worker, PeriodicTask
from .tui_auth import get_tui_internal_endpoints, verify_tui_token

AUTH_HEADER = "Authorization"
TOKEN_HEADER = "X-Reproq-Token"


def _get_stats_token():
    token = getattr(settings, "METRICS_AUTH_TOKEN", "") or os.environ.get("METRICS_AUTH_TOKEN", "")
    if token:
        return token
    return ""


def _token_from_request(request):
    auth = request.headers.get(AUTH_HEADER, "")
    if auth.lower().startswith("bearer "):
        return auth.split(" ", 1)[1].strip()
    return request.headers.get(TOKEN_HEADER, "")


def _authorized(request):
    token = _get_stats_token()
    candidate = _token_from_request(request)
    if token and candidate and hmac.compare_digest(candidate, token):
        return True
    if candidate and verify_tui_token(candidate):
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


def _proxy_target(kind):
    endpoints = get_tui_internal_endpoints()
    return endpoints.get(kind, "")


def _build_proxy_request(target_url):
    headers = {}
    token = _get_stats_token()
    if token:
        headers[AUTH_HEADER] = f"Bearer {token}"
    return urllib.request.Request(target_url, headers=headers)


def _proxy_response(request, target_url):
    if not target_url:
        return JsonResponse({"error": "metrics not configured"}, status=404)
    req = _build_proxy_request(target_url)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as err:
        body = err.read()
        content_type = err.headers.get("Content-Type", "text/plain")
        return HttpResponse(body, status=err.code, content_type=content_type)
    except urllib.error.URLError:
        return JsonResponse({"error": "metrics proxy failed"}, status=502)
    body = resp.read()
    content_type = resp.headers.get("Content-Type", "text/plain")
    return HttpResponse(body, status=resp.status, content_type=content_type)


def _stream_response(resp):
    try:
        while True:
            chunk = resp.read(1024)
            if not chunk:
                break
            yield chunk
    finally:
        resp.close()


def _proxy_stream(request, target_url):
    if not target_url:
        return JsonResponse({"error": "events not configured"}, status=404)
    req = _build_proxy_request(target_url)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as err:
        body = err.read()
        content_type = err.headers.get("Content-Type", "text/plain")
        return HttpResponse(body, status=err.code, content_type=content_type)
    except urllib.error.URLError:
        return JsonResponse({"error": "events proxy failed"}, status=502)
    response = StreamingHttpResponse(_stream_response(resp), status=resp.status)
    response["Content-Type"] = resp.headers.get("Content-Type", "text/event-stream")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@require_GET
def reproq_tui_metrics_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    target_url = _proxy_target("metrics")
    return _proxy_response(request, target_url)


@require_GET
def reproq_tui_health_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    target_url = _proxy_target("health")
    return _proxy_response(request, target_url)


@require_GET
def reproq_tui_events_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    target_url = _proxy_target("events")
    if target_url and request.META.get("QUERY_STRING"):
        target_url = f"{target_url}?{request.META.get('QUERY_STRING')}"
    return _proxy_stream(request, target_url)

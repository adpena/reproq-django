import hmac
import logging
import os
import socket
import time
import urllib.error
import urllib.request

from django.conf import settings
from django.db.models import Count
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.views.decorators.http import require_GET

from .models import TaskRun, Worker, PeriodicTask
from .tui_auth import get_tui_internal_endpoints, tui_events_enabled, verify_tui_token

AUTH_HEADER = "Authorization"
TOKEN_HEADER = "X-Reproq-Token"
logger = logging.getLogger(__name__)


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

def _client_ip(request):
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "")


def _request_id(request):
    return (
        request.META.get("HTTP_X_REQUEST_ID", "")
        or request.META.get("HTTP_X_RENDER_REQUEST_ID", "")
        or request.META.get("HTTP_X_AMZN_TRACE_ID", "")
    )


def _log_proxy_error(kind, request, target_url, err, status=None, duration_ms=None):
    logger.warning(
        "reproq_tui_proxy_error kind=%s status=%s duration_ms=%s target=%s client=%s request_id=%s err=%s",
        kind,
        status,
        duration_ms,
        target_url,
        _client_ip(request),
        _request_id(request),
        err,
    )


def _log_proxy_info(kind, request, target_url, status=None, duration_ms=None):
    logger.info(
        "reproq_tui_proxy kind=%s status=%s duration_ms=%s target=%s client=%s request_id=%s",
        kind,
        status,
        duration_ms,
        target_url,
        _client_ip(request),
        _request_id(request),
    )


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

    top_failing = TaskRun.objects.filter(status="FAILED").values("task_path").annotate(count=Count("result_id")).order_by("-count")[:5]

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
        "top_failing": list(top_failing),
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


def _proxy_response(request, target_url, kind):
    start = time.monotonic()
    if not target_url:
        _log_proxy_error(kind, request, target_url, "not_configured", status=404, duration_ms=0)
        return JsonResponse({"error": f"{kind} not configured"}, status=404)
    req = _build_proxy_request(target_url)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=err.code, duration_ms=duration_ms)
        body = err.read()
        content_type = err.headers.get("Content-Type", "text/plain")
        return HttpResponse(body, status=err.code, content_type=content_type)
    except (TimeoutError, socket.timeout) as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=504, duration_ms=duration_ms)
        return JsonResponse({"error": f"{kind} proxy timeout"}, status=504)
    except urllib.error.URLError as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=502, duration_ms=duration_ms)
        return JsonResponse({"error": f"{kind} proxy failed"}, status=502)
    duration_ms = int((time.monotonic() - start) * 1000)
    if duration_ms >= 1500:
        _log_proxy_info(kind, request, target_url, status=resp.status, duration_ms=duration_ms)
    body = resp.read()
    content_type = resp.headers.get("Content-Type", "text/plain")
    return HttpResponse(body, status=resp.status, content_type=content_type)


def _stream_response(resp, kind, request, target_url):
    try:
        while True:
            chunk = resp.read(1024)
            if not chunk:
                break
            yield chunk
    except (TimeoutError, socket.timeout) as err:
        _log_proxy_error(kind, request, target_url, err, status=504, duration_ms=None)
    except Exception as err:
        logger.exception(
            "reproq_tui_proxy_stream_error kind=%s target=%s client=%s request_id=%s err=%s",
            kind,
            target_url,
            _client_ip(request),
            _request_id(request),
            err,
        )
    finally:
        resp.close()


def _proxy_stream(request, target_url, kind):
    start = time.monotonic()
    if not target_url:
        _log_proxy_error(kind, request, target_url, "not_configured", status=404, duration_ms=0)
        return JsonResponse({"error": f"{kind} not configured"}, status=404)
    req = _build_proxy_request(target_url)
    try:
        timeout = 30 if kind == "events" else 5
        resp = urllib.request.urlopen(req, timeout=timeout)
    except urllib.error.HTTPError as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=err.code, duration_ms=duration_ms)
        body = err.read()
        content_type = err.headers.get("Content-Type", "text/plain")
        return HttpResponse(body, status=err.code, content_type=content_type)
    except (TimeoutError, socket.timeout) as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=504, duration_ms=duration_ms)
        return JsonResponse({"error": f"{kind} proxy timeout"}, status=504)
    except urllib.error.URLError as err:
        duration_ms = int((time.monotonic() - start) * 1000)
        _log_proxy_error(kind, request, target_url, err, status=502, duration_ms=duration_ms)
        return JsonResponse({"error": f"{kind} proxy failed"}, status=502)
    duration_ms = int((time.monotonic() - start) * 1000)
    _log_proxy_info(kind, request, target_url, status=resp.status, duration_ms=duration_ms)
    response = StreamingHttpResponse(_stream_response(resp, kind, request, target_url), status=resp.status)
    response["Content-Type"] = resp.headers.get("Content-Type", "text/event-stream")
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


@require_GET
def reproq_tui_metrics_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    target_url = _proxy_target("metrics")
    return _proxy_response(request, target_url, "metrics")


@require_GET
def reproq_tui_health_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    target_url = _proxy_target("health")
    return _proxy_response(request, target_url, "health")


@require_GET
def reproq_tui_events_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    if not tui_events_enabled():
        return JsonResponse({"error": "events disabled"}, status=404)
    target_url = _proxy_target("events")
    if target_url and request.META.get("QUERY_STRING"):
        target_url = f"{target_url}?{request.META.get('QUERY_STRING')}"
    return _proxy_stream(request, target_url, "events")

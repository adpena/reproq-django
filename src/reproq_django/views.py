import hmac
import logging
import os
import socket
import time
import urllib.error
import urllib.request

from django.conf import settings
from django.db import connections
from django.db.models import Count
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.utils import timezone
from datetime import timedelta
from django.views.decorators.http import require_GET

from .models import TaskRun, Worker, PeriodicTask, QueueControl
from .recurring import periodic_definition_status
from .db import default_db_alias, queue_db_aliases
from .tui_auth import (
    get_tui_internal_endpoints,
    tui_events_enabled,
    tui_low_memory_enabled,
    verify_tui_token,
)

AUTH_HEADER = "Authorization"
TOKEN_HEADER = "X-Reproq-Token"
logger = logging.getLogger(__name__)


def _get_stats_token():
    return getattr(settings, "METRICS_AUTH_TOKEN", "") or os.environ.get("METRICS_AUTH_TOKEN", "")


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


def _truthy(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _stats_db_aliases():
    override = getattr(settings, "REPROQ_STATS_DATABASES", None)
    if override:
        if isinstance(override, str):
            override = [override]
        aliases = []
        for alias in override:
            if alias == "*":
                aliases.extend(queue_db_aliases())
            else:
                aliases.append(alias)
        return sorted(set(aliases))
    return [default_db_alias()]


def _scheduler_status(aliases, periodic):
    from .pg_cron import is_available

    databases = []
    for alias in aliases:
        native = False
        error_type = None
        conn = connections[alias]
        if conn.vendor == 'postgresql':
            try:
                with conn.cursor() as cursor:
                    native = is_available(cursor)
            except Exception as error:
                native = None
                error_type = type(error).__name__
        rows = [row for row in periodic if row['database'] == alias and row['enabled']]
        databases.append({'alias': alias, 'native_extension_installed': native,
                          'capability_probe_error_type': error_type,
                          'registered_definitions': {provider: sum(
                              row['scheduler_provider'] == provider and row['definition_status'] == 'registered'
                              for row in rows) for provider in ('go', 'pg_cron')},
                          'execution_observed': False})
    return {'configured_mode': getattr(settings, 'REPROQ_SCHEDULER_MODE', '')
            or os.environ.get('REPROQ_SCHEDULER_MODE', '') or 'unspecified',
            'low_memory': tui_low_memory_enabled(), 'observed_state': 'unknown',
            'databases': databases,
            'note': 'Configuration, registration and extension installation do not prove scheduler execution.'}


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


def _low_memory_response(kind):
    return JsonResponse(
        {
            "error": f"{kind} disabled (LOW_MEMORY_MODE enabled)",
            "hint": "Unset LOW_MEMORY_MODE to re-enable metrics/health/events.",
        },
        status=503,
    )


def reproq_stats_api(request):
    """
    Returns JSON statistics about the Reproq queue and workers.
    """
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)

    aliases = _stats_db_aliases()
    tasks_totals = {}
    queues_totals = {}
    workers = []
    periodic = []
    queue_controls = []
    top_failing_counts = {}
    databases = []

    for alias in aliases:
        task_stats = TaskRun.objects.using(alias).values("status").annotate(count=Count("result_id"))
        queue_stats = TaskRun.objects.using(alias).values("queue_name", "status").annotate(count=Count("result_id"))
        worker_stats = Worker.objects.using(alias).all().values(
            "worker_id", "hostname", "concurrency", "queues", "last_seen_at", "version"
        )
        periodic_stats = PeriodicTask.objects.using(alias).all().values(
            "name", "cron_expr", "enabled", "next_run_at", "queue_name", "last_run_at",
            "scheduler_provider", "spec_version"
        )
        periodic_stats = list(periodic_stats)
        for row in periodic_stats:
            row["definition_status"] = periodic_definition_status(row["spec_version"], row["scheduler_provider"])
            if row["scheduler_provider"] == "pg_cron":
                row["next_run_at"] = None
                row["occurrence_clock"] = "UTC execution minute; scheduled by native pg_cron"
        controls = QueueControl.objects.using(alias).all().values(
            "queue_name", "paused", "paused_at", "reason", "updated_at"
        )
        top_failing = (
            TaskRun.objects.using(alias)
            .filter(status="FAILED")
            .values("task_path")
            .annotate(count=Count("result_id"))
            .order_by("-count")[:5]
        )

        for row in task_stats:
            tasks_totals[row["status"]] = tasks_totals.get(row["status"], 0) + row["count"]

        per_queue = {}
        for row in queue_stats:
            queue = row["queue_name"] or "default"
            status = row["status"]
            queues_totals.setdefault(queue, {})[status] = queues_totals.get(queue, {}).get(status, 0) + row["count"]
            per_queue.setdefault(queue, {})[status] = row["count"]

        for row in worker_stats:
            payload = dict(row)
            payload["database"] = alias
            workers.append(payload)

        for row in periodic_stats:
            payload = dict(row)
            payload["database"] = alias
            periodic.append(payload)

        for row in controls:
            payload = dict(row)
            payload["database"] = alias
            queue_controls.append(payload)

        for row in top_failing:
            task_path = row["task_path"] or ""
            top_failing_counts[task_path] = top_failing_counts.get(task_path, 0) + row["count"]

        databases.append(
            {
                "alias": alias,
                "tasks": {row["status"]: row["count"] for row in task_stats},
                "queues": per_queue,
                "workers": list(worker_stats),
                "periodic": list(periodic_stats),
            }
        )

    alive_cutoff = timezone.now() - timedelta(minutes=2)
    alive = sum(1 for worker in workers if worker.get("last_seen_at") and worker["last_seen_at"] > alive_cutoff)
    dead = len(workers) - alive

    top_failing_list = [
        {"task_path": key, "count": value}
        for key, value in sorted(top_failing_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    ]

    return JsonResponse({
        "tasks": tasks_totals,
        "queues": queues_totals,
        "workers": workers,
        "periodic": periodic,
        "queue_controls": queue_controls,
        "worker_health": {"alive": alive, "dead": dead},
        "scheduler": _scheduler_status(aliases, periodic),
        "top_failing": top_failing_list,
        "databases": databases,
    })



def _proxy_target(kind):
    endpoints = get_tui_internal_endpoints()
    return endpoints.get(kind, "")


def _build_proxy_request(target_url, request):
    headers = {}
    token = _get_stats_token()
    if token:
        headers[AUTH_HEADER] = f"Bearer {token}"
    else:
        incoming = request.headers.get(AUTH_HEADER, "")
        if incoming:
            headers[AUTH_HEADER] = incoming
    return urllib.request.Request(target_url, headers=headers)


def _proxy_response(request, target_url, kind):
    start = time.monotonic()
    if not target_url:
        _log_proxy_error(kind, request, target_url, "not_configured", status=404, duration_ms=0)
        return JsonResponse({"error": f"{kind} not configured"}, status=404)
    req = _build_proxy_request(target_url, request)
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
    req = _build_proxy_request(target_url, request)
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
    if tui_low_memory_enabled():
        return _low_memory_response("metrics")
    target_url = _proxy_target("metrics")
    return _proxy_response(request, target_url, "metrics")


@require_GET
def reproq_tui_health_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    if tui_low_memory_enabled():
        return _low_memory_response("health")
    target_url = _proxy_target("health")
    return _proxy_response(request, target_url, "health")


@require_GET
def reproq_tui_events_proxy(request):
    if not _authorized(request):
        return JsonResponse({"error": "Unauthorized"}, status=403)
    if tui_low_memory_enabled():
        return _low_memory_response("events")
    if not tui_events_enabled():
        return JsonResponse({"error": "events disabled"}, status=404)
    target_url = _proxy_target("events")
    if target_url and request.META.get("QUERY_STRING"):
        target_url = f"{target_url}?{request.META.get('QUERY_STRING')}"
    return _proxy_stream(request, target_url, "events")

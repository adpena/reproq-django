import base64
import hashlib
import hmac
import json
import os
import secrets
import time

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.contrib.auth.views import LoginView
from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt

PAIR_TTL_SECONDS = 10 * 60
TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60


def _get_tui_secret():
    return getattr(settings, "SECRET_KEY", "")


def _tui_enabled():
    return bool(_get_tui_secret())


def _get_tui_setting(name):
    return getattr(settings, name, "") or os.environ.get(name, "")


def _truthy(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def tui_low_memory_enabled():
    return _truthy(_get_tui_setting("LOW_MEMORY_MODE"))


def tui_events_enabled():
    if tui_low_memory_enabled():
        return False
    return not _truthy(_get_tui_setting("REPROQ_TUI_DISABLE_EVENTS"))


def _normalize_base_url(raw):
    if not raw:
        return ""
    raw = raw.strip()
    if "://" not in raw:
        raw = "http://" + raw
    if raw.startswith("http://:"):
        raw = "http://127.0.0.1:" + raw[len("http://:") :]
    if raw.startswith("https://:"):
        raw = "https://127.0.0.1:" + raw[len("https://:") :]
    return raw.rstrip("/")


def _join_url(base, suffix):
    base = base.rstrip("/")
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return base + suffix


def _derive_metrics_url(worker_url):
    if not worker_url:
        return ""
    return _join_url(worker_url, "/metrics")


def _derive_health_url(metrics_url):
    if not metrics_url:
        return ""
    trimmed = metrics_url.rstrip("/")
    if trimmed.endswith("/metrics"):
        trimmed = trimmed[: -len("/metrics")]
    return _join_url(trimmed, "/healthz")


def _derive_events_url(worker_url):
    if not worker_url:
        return ""
    return _join_url(worker_url, "/events")


def _public_worker_config():
    worker_url = _get_tui_setting("REPROQ_TUI_WORKER_URL")
    metrics_url = _get_tui_setting("REPROQ_TUI_WORKER_METRICS_URL")
    health_url = _get_tui_setting("REPROQ_TUI_WORKER_HEALTH_URL")
    events_url = _get_tui_setting("REPROQ_TUI_EVENTS_URL") if tui_events_enabled() else ""
    if not worker_url and metrics_url:
        trimmed = metrics_url.rstrip("/")
        if trimmed.endswith("/metrics"):
            worker_url = trimmed[: -len("/metrics")]
    if worker_url and not metrics_url:
        metrics_url = _derive_metrics_url(worker_url)
    if metrics_url and not health_url:
        health_url = _derive_health_url(metrics_url)
    if worker_url and not events_url and tui_events_enabled():
        events_url = _derive_events_url(worker_url)

    payload = {}
    if worker_url:
        payload["worker_url"] = worker_url
    if metrics_url:
        payload["worker_metrics_url"] = metrics_url
    if health_url:
        payload["worker_health_url"] = health_url
    if events_url:
        payload["events_url"] = events_url
    return payload


def get_tui_internal_endpoints():
    internal = _get_tui_setting("REPROQ_TUI_WORKER_INTERNAL_URL")
    if not internal:
        internal = _get_tui_setting("METRICS_ADDR")
    if not internal:
        port = _get_tui_setting("METRICS_PORT")
        if port:
            internal = f"127.0.0.1:{port}"
    if not internal:
        internal = "127.0.0.1:9090"
    internal = _normalize_base_url(internal)
    if not internal:
        return {}
    endpoints = {
        "metrics": _join_url(internal, "/metrics"),
        "health": _join_url(internal, "/healthz"),
        "events": _join_url(internal, "/events"),
    }
    if not tui_events_enabled():
        endpoints.pop("events", None)
    return endpoints


def build_tui_config_payload(request):
    payload = _public_worker_config()
    if payload:
        if tui_low_memory_enabled():
            payload["low_memory_mode"] = True
        return payload
    internal = get_tui_internal_endpoints()
    if not internal:
        return {}
    payload = {
        "worker_metrics_url": request.build_absolute_uri(reverse("reproq-tui-metrics")),
        "worker_health_url": request.build_absolute_uri(reverse("reproq-tui-health")),
        "events_url": request.build_absolute_uri(reverse("reproq-tui-events")),
    }
    if tui_low_memory_enabled():
        payload["low_memory_mode"] = True
    return payload


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data):
    pad = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + pad)


def _sign_token(payload, secret):
    header = {"alg": "HS256", "typ": "JWT"}
    header_json = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    header_b64 = _b64url(header_json)
    payload_b64 = _b64url(payload_json)
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


def _issue_token(user, secret):
    now = int(time.time())
    payload = {
        "sub": str(user.pk),
        "username": user.get_username(),
        "superuser": True,
        "iat": now,
        "exp": now + TOKEN_TTL_SECONDS,
        "iss": "reproq-django",
        "aud": "reproq-tui",
        "jti": secrets.token_hex(8),
    }
    token = _sign_token(payload, secret)
    return token, payload["exp"]


def verify_tui_token(token):
    secret = _get_tui_secret()
    if not secret or not token:
        return False
    parts = token.split(".")
    if len(parts) != 3:
        return False
    header_b64, payload_b64, signature_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    try:
        expected = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        signature = _b64url_decode(signature_b64)
    except (ValueError, TypeError):
        return False
    if not hmac.compare_digest(signature, expected):
        return False
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except (ValueError, TypeError):
        return False
    exp = payload.get("exp")
    if exp and int(exp) < int(time.time()):
        return False
    if payload.get("aud") not in ("reproq-tui", "reproq-worker"):
        return False
    if not payload.get("superuser"):
        return False
    return True


class TUILoginView(LoginView):
    template_name = "reproq_tui/login.html"


tui_login = TUILoginView.as_view()


def _pair_key(code):
    return f"reproq_tui_pair:{code}"


def _token_key(code):
    return f"reproq_tui_token:{code}"


def _load_pair(code):
    if not code:
        return None
    return cache.get(_pair_key(code))


@csrf_exempt
def tui_pair(request):
    if not _tui_enabled():
        return JsonResponse({"error": "tui auth disabled"}, status=404)
    if request.method not in ("GET", "POST"):
        return JsonResponse({"error": "method not allowed"}, status=405)
    code = secrets.token_hex(4)
    cache.set(_pair_key(code), {"status": "pending", "created_at": int(time.time())}, timeout=PAIR_TTL_SECONDS)
    verify_url = request.build_absolute_uri(reverse("reproq-tui-authorize")) + f"?code={code}"
    expires_at = int(time.time()) + PAIR_TTL_SECONDS
    payload = {
        "code": code,
        "verify_url": verify_url,
        "expires_at": expires_at,
    }
    payload.update(build_tui_config_payload(request))
    return JsonResponse(payload)


def tui_pair_status(request, code):
    if not _tui_enabled():
        return JsonResponse({"error": "tui auth disabled"}, status=404)
    pair = _load_pair(code)
    if not pair:
        return JsonResponse({"status": "expired"}, status=404)
    token_payload = cache.get(_token_key(code))
    if token_payload:
        return JsonResponse({
            "status": "approved",
            "token": token_payload["token"],
            "expires_at": token_payload["expires_at"],
        })
    return JsonResponse({"status": "pending"})


def tui_config(request):
    if not _tui_enabled():
        return JsonResponse({"error": "tui auth disabled"}, status=404)
    payload = build_tui_config_payload(request)
    return JsonResponse(payload)


@login_required(login_url="reproq-tui-login")
def tui_authorize(request):
    if not _tui_enabled():
        return JsonResponse({"error": "tui auth disabled"}, status=404)
    if not request.user.is_superuser:
        return JsonResponse({"error": "superuser required"}, status=403)
    code = request.GET.get("code", "") or request.POST.get("code", "")
    pair = _load_pair(code)
    if not pair:
        return render(request, "reproq_tui/authorize.html", {"status": "expired", "code": code})
    if request.method == "POST":
        token, expires_at = _issue_token(request.user, _get_tui_secret())
        cache.set(_token_key(code), {"token": token, "expires_at": expires_at}, timeout=5 * 60)
        cache.set(_pair_key(code), {"status": "approved", "created_at": pair.get("created_at", 0)}, timeout=PAIR_TTL_SECONDS)
        return render(request, "reproq_tui/authorize.html", {
            "status": "approved",
            "code": code,
            "expires_at": expires_at,
        })
    return render(request, "reproq_tui/authorize.html", {"status": "pending", "code": code})

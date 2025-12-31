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
    return getattr(settings, "REPROQ_TUI_SECRET", "") or os.environ.get("REPROQ_TUI_SECRET", "")


def _tui_enabled():
    return bool(_get_tui_secret())


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
    return JsonResponse({
        "code": code,
        "verify_url": verify_url,
        "expires_at": expires_at,
    })


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

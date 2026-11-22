from types import SimpleNamespace
import json

from django.test import RequestFactory
from django.urls import resolve, reverse

from reproq_django import tui_auth


def assert_private(response, status):
    assert response.status_code == status
    controls = set(response["Cache-Control"].split(", "))
    assert {"private", "no-store", "max-age=0"} <= controls


def test_pairing_credentials_and_authorization_are_not_cached(settings):
    settings.ROOT_URLCONF = "reproq_django.urls"
    settings.METRICS_AUTH_TOKEN = "synthetic-pair-key"
    settings.TEMPLATES = [{
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
    }]
    factory = RequestFactory()
    issue_path = reverse("reproq-tui-pair")
    authorize_path = reverse("reproq-tui-authorize")
    assert resolve(issue_path).func.csrf_exempt is True
    assert not getattr(resolve(authorize_path).func, "csrf_exempt", False)

    for issue in (factory.get, factory.post):
        response = tui_auth.tui_pair(issue(issue_path))
        assert_private(response, 200)
        code = json.loads(response.content)["code"]

    poll = factory.get(reverse("reproq-tui-pair-status", kwargs={"code": code}))
    pending = tui_auth.tui_pair_status(poll, code)
    assert_private(pending, 200)
    assert json.loads(pending.content) == {"status": "pending"}

    preview = factory.get(authorize_path, {"code": code})
    preview.user = SimpleNamespace(is_authenticated=False)
    assert_private(tui_auth.tui_authorize(preview), 302)
    preview.user = SimpleNamespace(is_authenticated=True, is_superuser=False)
    assert_private(tui_auth.tui_authorize(preview), 403)

    operator = SimpleNamespace(is_authenticated=True, is_superuser=True, pk=1, get_username=lambda: "operator")
    preview.user = operator
    assert_private(tui_auth.tui_authorize(preview), 200)
    approve = factory.post(authorize_path, {"code": code})
    approve.user = operator
    assert_private(tui_auth.tui_authorize(approve), 200)

    approved = tui_auth.tui_pair_status(poll, code)
    assert_private(approved, 200)
    payload = json.loads(approved.content)
    assert payload["status"] == "approved" and payload["token"]

    assert_private(tui_auth.tui_pair_status(poll, "expired-test-code"), 404)
    assert_private(tui_auth.tui_pair(factory.delete(issue_path)), 405)


def test_disabled_pairing_responses_are_not_cached(settings, monkeypatch):
    settings.METRICS_AUTH_TOKEN = ""
    monkeypatch.delenv("METRICS_AUTH_TOKEN", raising=False)
    request = RequestFactory().get("/tui/pair/")
    request.user = SimpleNamespace(is_authenticated=True)
    assert_private(tui_auth.tui_pair(request), 404)
    assert_private(tui_auth.tui_pair_status(request, "test-code"), 404)
    assert_private(tui_auth.tui_authorize(request), 404)

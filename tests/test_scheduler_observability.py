import json
from datetime import timedelta
from io import StringIO
from types import SimpleNamespace

import pytest
from django.core.management import call_command
from django.test import RequestFactory
from django.utils import timezone

from reproq_django.models import PeriodicTask, Worker
from reproq_django.recurring import scheduler_status
from reproq_django.views import reproq_stats_api


@pytest.mark.parametrize("mode,command,expected", [
    (None, None, True), ("  BeAt  ", None, True),
    ("beat", "", False), ("beat", " OFF ", False),
    ("off", None, False), ("pg_cron", None, False),
    ("beat", "custom command with private arguments", True),
])
def test_status_matches_managed_beat_configuration_without_exposing_commands(
    monkeypatch, mode, command, expected,
):
    monkeypatch.setenv("LOW_MEMORY_MODE", "1")
    for name, value in [("REPROQ_SCHEDULER_MODE", mode), ("REPROQ_BEAT_CMD", command)]:
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    result = scheduler_status([])
    assert result["managed_beat_configured"] is expected
    assert result["execution_observed"] is False
    assert "private arguments" not in json.dumps(result)


def test_stats_report_database_clock_overdue_and_disabled_definitions_without_writes(monkeypatch):
    call_command("migrate", verbosity=0)
    PeriodicTask.objects.all().delete()
    now = timezone.now()
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "beat")
    monkeypatch.setenv("REPROQ_BEAT_CMD", "off")
    try:
        for name, provider, version, enabled, when in [
            ("overdue", "go", 1, True, now - timedelta(days=3)),
            ("future", "go", 1, True, now + timedelta(days=1)),
            ("disabled", "go", 1, False, now - timedelta(days=4)),
            ("native", "pg_cron", 1, True, now - timedelta(days=5)),
            ("legacy", "unassigned", 0, True, now - timedelta(days=6)),
        ]:
            PeriodicTask.objects.create(name=name, cron_expr="0 0 * * *", task_path="owned.task",
                                        next_run_at=when, scheduler_provider=provider,
                                        spec_version=version, enabled=enabled)
        before = list(PeriodicTask.objects.order_by("name").values())
        # A skewed application clock must not become the scheduler's queue clock.
        monkeypatch.setattr("django.utils.timezone.now", lambda: now + timedelta(days=10))
        request = RequestFactory().get("/stats/")
        request.user = SimpleNamespace(is_authenticated=True, is_staff=True)
        payload = json.loads(reproq_stats_api(request).content)
        observation = payload["scheduler"]["databases"][0]
        assert observation["enabled_definitions"] == 4
        assert observation["disabled_definitions"] == 1
        assert observation["reconciliation_required"] == 1
        assert observation["registered_definitions"] == {"go": 2, "pg_cron": 1}
        assert observation["overdue_go_definitions"] == 1
        assert 259_199 < observation["oldest_overdue_go_seconds"] < 345_600
        assert observation["managed_beat_configured"] is False
        assert observation["managed_beat_disabled_reason"] == "explicit_beat_command"
        assert payload["scheduler"]["observed_state"] == "unknown"
        assert observation["execution_observed"] is False
        assert any("managed beat disabled" in warning for warning in payload["scheduler"]["warnings"])
        assert any("past-due cursors" in warning for warning in payload["scheduler"]["warnings"])
        assert "observed_at" in payload["periodic"][0]
        assert list(PeriodicTask.objects.order_by("name").values()) == before
    finally:
        PeriodicTask.objects.all().delete()


def test_disabled_definitions_do_not_imply_operator_intent_or_stalled_execution(monkeypatch):
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "off")
    now = timezone.now()
    result = scheduler_status([{
        "enabled": False, "scheduler_provider": "go", "spec_version": 1,
        "next_run_at": now - timedelta(days=1), "observed_at": now,
    }])
    assert result["disabled_definitions"] == 1
    assert result["overdue_go_definitions"] == 0
    assert result["warnings"] == []
    assert result["execution_observed"] is False
    assert "held" not in json.dumps(result)


@pytest.mark.parametrize("skew_minutes", [-10, 10])
def test_api_and_cli_use_database_clock_for_nonempty_worker_heartbeats(monkeypatch, skew_minutes):
    call_command("migrate", verbosity=0)
    Worker.objects.all().delete()
    PeriodicTask.objects.all().delete()
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "off")
    now = timezone.now()
    try:
        Worker.objects.create(worker_id="fresh", hostname="owned", concurrency=1)
        Worker.objects.create(worker_id="old", hostname="owned", concurrency=1)
        Worker.objects.filter(worker_id="old").update(last_seen_at=now - timedelta(days=1))
        PeriodicTask.objects.create(name="due", cron_expr="0 0 * * *", task_path="owned.task",
                                    scheduler_provider="go", spec_version=1,
                                    next_run_at=now - timedelta(days=1))
        monkeypatch.setattr("django.utils.timezone.now", lambda: now + timedelta(minutes=skew_minutes))
        request = RequestFactory().get("/stats/")
        request.user = SimpleNamespace(is_authenticated=True, is_staff=True)
        payload = json.loads(reproq_stats_api(request).content)
        assert payload["worker_health"] == {"alive": 1, "dead": 1}
        assert all("observed_at" in row for row in payload["workers"])
        output = StringIO()
        call_command("reproq", "status", stdout=output)
        text = output.getvalue()
        assert "Worker records: 2 (alive: 1, stale: 1)" in text
        assert "Active Workers" not in text
        assert "Scheduler execution: unknown" in text
        assert "1 Go cursor(s) past due" in text
        assert "managed beat disabled" in text
    finally:
        Worker.objects.all().delete()
        PeriodicTask.objects.all().delete()

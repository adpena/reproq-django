from __future__ import annotations

from reproq_django import tui_auth, views


def test_low_memory_mode_env_toggle(monkeypatch):
    monkeypatch.setenv("LOW_MEMORY_MODE", "1")
    assert tui_auth.tui_low_memory_enabled() is True
    monkeypatch.setenv("LOW_MEMORY_MODE", "0")
    assert tui_auth.tui_low_memory_enabled() is False


def test_events_disabled_in_low_memory(monkeypatch):
    monkeypatch.setenv("LOW_MEMORY_MODE", "true")
    monkeypatch.delenv("REPROQ_TUI_DISABLE_EVENTS", raising=False)
    assert tui_auth.tui_events_enabled() is False


def test_stats_token_uses_metrics_auth_token(monkeypatch):
    monkeypatch.setenv("METRICS_AUTH_TOKEN", "metrics-token")
    assert views._get_stats_token() == "metrics-token"


def test_stats_token_empty_when_unset(monkeypatch):
    monkeypatch.delenv("METRICS_AUTH_TOKEN", raising=False)
    assert views._get_stats_token() == ""


def test_beat_configured_defaults_when_mode_is_beat(monkeypatch):
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "beat")
    monkeypatch.delenv("REPROQ_BEAT_CMD", raising=False)
    assert views._beat_configured() is True


def test_beat_configured_disabled_when_mode_is_cron(monkeypatch):
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "cron")
    monkeypatch.setenv("REPROQ_BEAT_CMD", "uv run python manage.py reproq beat")
    assert views._beat_configured() is False


def test_beat_configured_respects_disabled_cmd(monkeypatch):
    monkeypatch.setenv("REPROQ_SCHEDULER_MODE", "beat")
    monkeypatch.setenv("REPROQ_BEAT_CMD", "off")
    assert views._beat_configured() is False

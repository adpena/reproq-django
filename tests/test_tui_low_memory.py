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


def test_scheduler_configuration_does_not_claim_observed_execution(monkeypatch):
    monkeypatch.setenv('REPROQ_SCHEDULER_MODE', 'pg_cron')
    monkeypatch.setenv('LOW_MEMORY_MODE', '1')
    result = views._scheduler_status(['default'], [])
    assert result['configured_mode'] == 'pg_cron'
    assert result['observed_state'] == 'unknown'
    assert result['databases'][0]['native_extension_installed'] is False
    assert result['databases'][0]['execution_observed'] is False
    assert result['databases'][0]['registered_definitions'] == {'go': 0, 'pg_cron': 0}

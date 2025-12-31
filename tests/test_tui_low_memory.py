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


def test_stats_token_falls_back_to_tui_secret(monkeypatch):
    monkeypatch.delenv("METRICS_AUTH_TOKEN", raising=False)
    monkeypatch.setenv("REPROQ_TUI_SECRET", "shared-secret")
    assert views._get_stats_token() == "shared-secret"


def test_stats_token_prefers_metrics_token(monkeypatch):
    monkeypatch.setenv("METRICS_AUTH_TOKEN", "metrics-token")
    monkeypatch.setenv("REPROQ_TUI_SECRET", "shared-secret")
    assert views._get_stats_token() == "metrics-token"

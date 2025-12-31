from __future__ import annotations

from reproq_django import tui_auth


def test_low_memory_mode_env_toggle(monkeypatch):
    monkeypatch.setenv("LOW_MEMORY_MODE", "1")
    assert tui_auth.tui_low_memory_enabled() is True
    monkeypatch.setenv("LOW_MEMORY_MODE", "0")
    assert tui_auth.tui_low_memory_enabled() is False


def test_events_disabled_in_low_memory(monkeypatch):
    monkeypatch.setenv("LOW_MEMORY_MODE", "true")
    monkeypatch.delenv("REPROQ_TUI_DISABLE_EVENTS", raising=False)
    assert tui_auth.tui_events_enabled() is False


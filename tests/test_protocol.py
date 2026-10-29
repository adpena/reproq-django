import io
import json
import math

import pytest

from reproq_django.protocol import (
    ProtocolLimitError,
    encode_envelope,
    failure_envelope,
    read_payload,
)


@pytest.mark.parametrize("value", [None, False, 0, -1, '😀\n"\\', [1, {"x": "Texas"}]])
def test_envelope_roundtrip_accounts_for_entire_utf8_frame(value):
    payload = {"ok": True, "return": value}
    encoded = encode_envelope(payload, 1024)
    assert json.loads(encoded) == payload
    assert encoded.endswith(b"\n")
    assert encode_envelope(payload, len(encoded)) == encoded
    with pytest.raises(ProtocolLimitError):
        encode_envelope(payload, len(encoded) - 1)


@pytest.mark.parametrize(
    "value", ["x" * 100_000, "😀" * 100_000, "\x00" * 1000, list(range(10_000))]
)
def test_oversized_results_fail_without_publishing_a_success_prefix(value):
    with pytest.raises(ProtocolLimitError):
        encode_envelope({"ok": True, "return": value}, 1024)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, {1: "key"}, object()])
def test_invalid_json_values_are_explicit_failures(value):
    with pytest.raises((TypeError, ValueError)):
        encode_envelope({"ok": True, "return": value}, 1024)


def test_cyclic_and_deep_results_fail_with_bounded_traversal():
    circular = []
    circular.append(circular)
    with pytest.raises(ValueError, match="Circular"):
        encode_envelope({"return": circular}, 1024)
    deep = None
    for _ in range(65):
        deep = [deep]
    with pytest.raises(ProtocolLimitError, match="nesting"):
        encode_envelope({"return": deep}, 1024)


def test_failure_can_fit_the_minimal_complete_frame_without_stringifying_error():
    class UnprintableError(Exception):
        def __str__(self):
            pytest.fail("Unbounded exception stringification")

    assert failure_envelope(UnprintableError(), 13) == b'{"ok":false}\n'
    assert json.loads(failure_envelope(UnprintableError(), 256))[
        "exception_class"
    ].endswith(".UnprintableError")


def test_payload_is_a_single_bounded_object():
    assert read_payload(io.BytesIO(b'{"x":1}'), 7) == {"x": 1}
    with pytest.raises(ProtocolLimitError):
        read_payload(io.BytesIO(b'{"x":1}'), 6)
    for payload in (b"{}{}", b"[]", b"null"):
        with pytest.raises(ValueError):
            read_payload(io.BytesIO(payload), 100)


def test_large_exception_message_keeps_identity_with_explicit_bounded_diagnostics():
    import tracemalloc

    try:
        raise ValueError("x" * (4 * 1024 * 1024))
    except ValueError as error:
        tracemalloc.start()
        try:
            response = failure_envelope(error, 1024)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
    envelope = json.loads(response)
    assert len(response) <= 1024 and peak < 128 * 1024
    assert envelope["exception_class"] == "builtins.ValueError"
    assert (
        "unavailable" in (envelope.get("traceback", "") + envelope["message"]).lower()
    )


def test_unencodable_exception_message_keeps_a_complete_bounded_failure():
    try:
        raise ValueError("owned\ud800error")
    except ValueError as error:
        response = failure_envelope(error, 2048)
    payload = json.loads(response)
    assert payload["ok"] is False
    assert payload["exception_class"] == "builtins.ValueError"
    assert "unavailable" in payload["traceback"].lower()
    assert len(response) <= 2048


@pytest.mark.parametrize("module", [None, 123, "owned\ud800module"])
def test_unavailable_exception_identity_uses_minimal_complete_failure(module, capsys):
    from reproq_django.executor import _diagnostic

    exception_type = type("OwnedError", (ValueError,), {"__module__": module})
    try:
        raise exception_type("owned")
    except ValueError as error:
        _diagnostic(error)
        response = failure_envelope(error, 2048)
    assert response == b'{"ok":false}\n'
    assert "unavailable" in capsys.readouterr().err.lower()


def test_executor_diagnostic_does_not_load_or_print_unbounded_source(
    tmp_path, monkeypatch, capsys,
):
    import linecache
    from reproq_django.executor import _diagnostic

    def forbid_source_read(*args, **kwargs):
        pytest.fail("Diagnostic formatter loaded source text")

    source = tmp_path / "owned_diagnostic.py"
    text = "raise ValueError('owned error') #" + "x" * (256 * 1024)
    source.write_text(text)
    monkeypatch.setattr(linecache, "getlines", forbid_source_read)
    try:
        exec(compile(text, str(source), "exec"), {})
    except ValueError as error:
        _diagnostic(error)
    diagnostic = capsys.readouterr().err
    assert len(diagnostic.encode("utf-8")) <= 16416
    assert "builtins.ValueError: owned error" in diagnostic

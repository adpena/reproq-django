"""Bounded, complete UTF-8 envelopes for the worker/executor protocol."""

import io
import json
import math


class ProtocolLimitError(ValueError):
    pass


class WorkerExecutionError(RuntimeError):
    """Worker/protocol failure without a recoverable Python task exception."""


def _preflight(value, limit):
    # Bound the encoder's individual string chunks, nesting and traversal before
    # it can allocate an escaped copy of an arbitrarily large task return value.
    remaining = limit
    active = set()

    def consume(size):
        nonlocal remaining
        remaining -= size
        if remaining < 0:
            raise ProtocolLimitError("Result exceeds the worker output byte limit")

    def visit(item, depth=0):
        if depth > 64:
            raise ProtocolLimitError("Result exceeds the protocol nesting limit")
        if item is None or isinstance(item, bool):
            consume(4 if item is None or item is True else 5)
        elif isinstance(item, str):
            consume(2)
            if len(item) > remaining:
                raise ProtocolLimitError("Result exceeds the worker output byte limit")
            for start in range(0, len(item), 4096):
                consume(len(item[start : start + 4096].encode("utf-8")))
        elif isinstance(item, int):
            # Avoid str(huge_integer) allocating outside the worker budget.
            if item.bit_length() > max(1, remaining) * 4:
                raise ProtocolLimitError(
                    "Result integer exceeds the worker output byte limit"
                )
            consume(len(str(item)))
        elif isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError("Non-finite numbers are not valid protocol JSON")
            consume(len(str(item)))
        elif isinstance(item, (dict, list, tuple)):
            identity = id(item)
            if identity in active:
                raise ValueError("Circular result is not valid protocol JSON")
            active.add(identity)
            consume(2)
            for index, entry in enumerate(item):
                if index:
                    consume(1)
                if isinstance(item, dict):
                    if not isinstance(entry, str):
                        raise TypeError("Protocol JSON object keys must be strings")
                    visit(entry, depth + 1)
                    consume(1)
                    visit(item[entry], depth + 1)
                else:
                    visit(entry, depth + 1)
            active.remove(identity)
        else:
            raise TypeError(
                f"Return value of type {type(item).__name__} is not JSON serializable"
            )

    visit(value)


def encode_envelope(payload, max_bytes):
    """Encode all framing within the worker's limit, or emit no bytes at all."""
    if max_bytes <= 0:
        raise ValueError("The worker output byte limit must be positive")
    _preflight(payload, max_bytes - 1)
    encoded = io.BytesIO()
    encoder = json.JSONEncoder(
        ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )
    for chunk in encoder.iterencode(payload):
        data = chunk.encode("utf-8")
        if encoded.tell() + len(data) + 1 > max_bytes:
            raise ProtocolLimitError("Result exceeds the worker output byte limit")
        encoded.write(data)
    encoded.write(b"\n")
    return encoded.getvalue()


def _exception_class_path(error: BaseException, max_chars: int) -> str | None:
    cls = type(error)
    module, name = cls.__module__, cls.__qualname__
    if (
        not isinstance(module, str)
        or not isinstance(name, str)
        or len(module) + len(name) + 1 > max_chars
    ):
        return None
    return f"{module}.{name}"


def bounded_traceback(error: BaseException, max_bytes: int) -> str:
    """Bound stack traversal without locals, source loading or custom __str__."""
    unavailable = "Traceback unavailable: diagnostic budget exceeded."
    remaining = min(max_bytes, 16384)
    parts = []

    def append(part):
        nonlocal remaining
        if len(part) > remaining:
            return False
        try:
            size = len(part.encode("utf-8"))
        except UnicodeError:
            return False
        if size > remaining:
            return False
        remaining -= size
        parts.append(part)
        return True

    if not append(
        "Traceback (task frames; source lines and chained details omitted):\n"
    ):
        return unavailable
    current = error.__traceback__
    if current is None:
        return "Traceback unavailable: exception has no raised stack."
    for _ in range(32):
        code = current.tb_frame.f_code
        if len(code.co_filename) + len(code.co_name) > remaining:
            return unavailable
        if not append(
            f'  File "{code.co_filename}", line {current.tb_lineno}, in {code.co_name}\n'
        ):
            return unavailable
        current = current.tb_next
        if current is None:
            break
    if current is not None:
        return "Traceback unavailable: frame limit exceeded."
    cls = type(error)
    path = _exception_class_path(error, remaining)
    if path is None:
        return "Traceback unavailable: exception class identity is unavailable."
    if not append(path):
        return unavailable
    if (
        cls.__str__ is BaseException.__str__
        and len(error.args) == 1
        and type(error.args[0]) is str
    ):
        if not append(": ") or not append(error.args[0]):
            return unavailable
    elif error.args:
        if not append("\n[Exception message omitted: custom or unbounded formatting.]"):
            return unavailable
    return "".join(parts)


def failure_envelope(error: BaseException, max_bytes: int) -> bytes:
    # Bound class identity before concatenating unusually large dynamic names.
    path = _exception_class_path(error, max_bytes)
    if path is None:
        return encode_envelope({"ok": False}, max_bytes)
    payload = {
        "ok": False,
        "exception_class": path,
        "message": "Task execution failed",
        "traceback": bounded_traceback(error, max_bytes),
    }
    try:
        return encode_envelope(payload, max_bytes)
    except (ProtocolLimitError, UnicodeError):
        payload.pop("traceback")
        payload["message"] = "Traceback unavailable: worker output budget exceeded."
        try:
            return encode_envelope(payload, max_bytes)
        except (ProtocolLimitError, UnicodeError):
            return encode_envelope({"ok": False}, max_bytes)


def read_payload(stream, max_bytes):
    if max_bytes <= 0:
        raise ValueError("The worker payload byte limit must be positive")
    data = stream.read(max_bytes + 1)
    if len(data) > max_bytes:
        raise ProtocolLimitError("Payload exceeds the worker payload byte limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("Task payload must be a JSON object")
    return value

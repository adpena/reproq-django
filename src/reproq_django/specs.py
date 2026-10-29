"""The task-spec and hash authority shared by all SDK enqueue providers."""

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from django.tasks.base import TASK_MAX_PRIORITY, TASK_MIN_PRIORITY

from .serialization import canonical_json, encode_args_kwargs

SPEC_VERSION = 1
PG_INT_MAX = 2**31 - 1


def _integer(name, value, minimum):
    if type(value) is not int or not minimum <= value <= PG_INT_MAX:
        raise ValueError(
            f"{name} must be an integer between {minimum} and {PG_INT_MAX}"
        )


def _text(name, value, optional=False):
    if optional and value is None:
        return
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be a nonempty string without NUL characters")


@dataclass(frozen=True)
class CanonicalSpec:
    payload: dict
    text: str
    sha256: str
    version: int = SPEC_VERSION


def _validate_payload(payload):
    fields = {
        "v",
        "task_path",
        "backend_alias",
        "args",
        "kwargs",
        "takes_context",
        "queue_name",
        "priority",
        "lock_key",
        "concurrency_key",
        "concurrency_limit",
        "run_after",
        "exec",
        "provenance",
    }
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ValueError("Unknown or incomplete canonical spec fields")
    if type(payload["v"]) is not int or payload["v"] != SPEC_VERSION:
        raise ValueError("Unsupported task spec version")
    execution, provenance = payload["exec"], payload["provenance"]
    if not isinstance(execution, dict) or set(execution) != {
        "timeout_seconds",
        "max_attempts",
    }:
        raise ValueError("Unknown or incomplete execution options")
    if not isinstance(provenance, dict) or set(provenance) != {
        "code_ref",
        "pip_lock_hash",
    }:
        raise ValueError("Unknown or incomplete task provenance")
    for name in ("task_path", "queue_name", "backend_alias"):
        _text(name, payload[name])
    for name in ("lock_key", "concurrency_key"):
        _text(name, payload[name], optional=True)
    for name in ("code_ref", "pip_lock_hash"):
        _text(name, provenance[name], optional=True)
    priority = payload["priority"]
    if (
        type(priority) is not int
        or not TASK_MIN_PRIORITY <= priority <= TASK_MAX_PRIORITY
    ):
        raise ValueError(
            f"priority must be an integer between {TASK_MIN_PRIORITY} and {TASK_MAX_PRIORITY}"
        )
    _integer("timeout_seconds", execution["timeout_seconds"], 1)
    _integer("max_attempts", execution["max_attempts"], 1)
    _integer("concurrency_limit", payload["concurrency_limit"], 0)
    if type(payload["takes_context"]) is not bool:
        raise ValueError("takes_context must be boolean")
    if not isinstance(payload["args"], list):
        raise ValueError("Encoded args must be an array")
    if not isinstance(payload["kwargs"], dict) or any(
        not isinstance(key, str) for key in payload["kwargs"]
    ):
        raise ValueError("Encoded kwargs must be a dictionary with string keys")
    if payload["run_after"] is not None:
        if not isinstance(payload["run_after"], str):
            raise ValueError("Encoded run_after must be an aware ISO datetime")
        value = datetime.fromisoformat(payload["run_after"])
        if value.utcoffset() is None:
            raise ValueError("run_after must be timezone-aware")


def read_spec(text, expected_sha256, version):
    """Validate stored canonical bytes without decoding refs or re-encoding args."""
    if type(version) is not int or version != SPEC_VERSION or not isinstance(text, str):
        raise ValueError("Unsupported or absent canonical task spec")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if digest != expected_sha256:
        raise ValueError("Canonical task spec checksum mismatch")
    payload = json.loads(text)
    _validate_payload(payload)
    if canonical_json(payload) != text:
        raise ValueError("Stored task spec is not canonical JSON")
    return CanonicalSpec(payload, text, digest, version)


def _validate_columns(spec, row, names):
    if any(getattr(row, name) != spec.payload[name] for name in names):
        raise ValueError("Canonical task source and delivery columns disagree")
    if row.max_attempts != spec.payload["exec"]["max_attempts"]:
        raise ValueError("Canonical task source and retry budget disagree")


def read_task_spec(row):
    """Validate native source once; JSONB is a projection, not argument authority."""
    spec = read_spec(row.canonical_spec_text, row.spec_hash, row.spec_version)
    _validate_columns(
        spec,
        row,
        (
            "task_path",
            "backend_alias",
            "queue_name",
            "priority",
            "lock_key",
            "concurrency_key",
            "concurrency_limit",
        ),
    )
    # TaskRun.run_after is mutable eligibility (periodic slots/backoff/replay).
    # The original native Task.run_after remains in immutable canonical source.
    if row.timeout_seconds != spec.payload["exec"]["timeout_seconds"]:
        raise ValueError("Canonical task source and execution columns disagree")
    return spec


def read_periodic_spec(row):
    """The same native contract applies before SDK native-job registration."""
    spec = read_spec(row.canonical_spec_text, row.canonical_spec_hash, row.spec_version)
    _validate_columns(
        spec,
        row,
        ("task_path", "queue_name", "priority", "concurrency_key", "concurrency_limit"),
    )
    if spec.payload["run_after"] is not None:
        raise ValueError("Periodic task source must not carry a one-off run_after")
    return spec


def build_spec(
    *,
    task_path,
    args=(),
    kwargs=None,
    takes_context=False,
    backend_alias="default",
    queue_name="default",
    priority=0,
    lock_key=None,
    concurrency_key=None,
    concurrency_limit=0,
    run_after=None,
    timeout_seconds=900,
    max_attempts=3,
    code_ref=None,
    pip_lock_hash=None,
):
    if not isinstance(args, (tuple, list)):
        raise ValueError("args must be a tuple or list")
    if kwargs is None:
        kwargs = {}
    if not isinstance(kwargs, dict) or any(not isinstance(key, str) for key in kwargs):
        raise ValueError("kwargs must be a dictionary with string keys")
    if run_after is not None and (
        not isinstance(run_after, datetime) or run_after.utcoffset() is None
    ):
        raise ValueError("run_after must be a timezone-aware datetime")
    encoded_args, encoded_kwargs = encode_args_kwargs(tuple(args), kwargs)
    payload = {
        "v": 1,
        "task_path": task_path,
        "backend_alias": backend_alias,
        "args": encoded_args,
        "kwargs": encoded_kwargs,
        "takes_context": takes_context,
        "queue_name": queue_name,
        "priority": priority,
        "lock_key": lock_key,
        "concurrency_key": concurrency_key,
        "concurrency_limit": concurrency_limit,
        "run_after": run_after.isoformat() if run_after else None,
        "exec": {"timeout_seconds": timeout_seconds, "max_attempts": max_attempts},
        "provenance": {"code_ref": code_ref, "pip_lock_hash": pip_lock_hash},
    }
    _validate_payload(payload)
    text = canonical_json(payload)
    return CanonicalSpec(
        json.loads(text), text, hashlib.sha256(text.encode("utf-8")).hexdigest()
    )


def spec_fields(spec):
    return {
        "spec_json": spec.payload,
        "spec_hash": spec.sha256,
        "canonical_spec_text": spec.text,
        "spec_version": spec.version,
    }

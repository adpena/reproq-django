from decimal import Decimal
from pathlib import Path
from uuid import UUID

from reproq_django.serialization import canonical_json, spec_hash_for


def test_spec_hash_contract():
    root = Path(__file__).resolve().parents[1]
    canonical_path = root / "contracts" / "spec_hash.json"
    hash_path = root / "contracts" / "spec_hash.sha256"

    expected_canonical = canonical_path.read_text().strip()
    expected_hash = hash_path.read_text().strip()

    spec = {
        "v": 1,
        "task_path": "contracts.example_task",
        "args": [1, {"nested": True}],
        "kwargs": {
            "amount": Decimal("12.34"),
            "token": UUID("123e4567-e89b-12d3-a456-426614174000"),
            "notes": "hello",
        },
        "takes_context": False,
        "queue_name": "default",
        "priority": 5,
        "lock_key": "user:123",
        "concurrency_key": "account:42",
        "concurrency_limit": 2,
        "run_after": "2025-01-01T12:00:00+00:00",
        "exec": {"timeout_seconds": 900, "max_attempts": 3},
        "provenance": {"code_ref": "abc123", "pip_lock_hash": "deadbeef"},
    }

    canonical = canonical_json(spec)
    assert canonical == expected_canonical
    assert spec_hash_for(spec) == expected_hash

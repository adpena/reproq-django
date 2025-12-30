import hashlib
import json
from typing import Any

from django.core.serializers.json import DjangoJSONEncoder


def canonical_json(obj: Any) -> str:
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        cls=DjangoJSONEncoder,
    )


def normalize_json(obj: Any) -> Any:
    return json.loads(canonical_json(obj))


def spec_hash_for(spec: Any) -> str:
    return hashlib.sha256(canonical_json(spec).encode("utf-8")).hexdigest()

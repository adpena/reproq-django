import hashlib
import json
from datetime import timedelta
from typing import Any

from django.apps import apps
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Model


TYPE_MARKER = "__reproq_type__"


class DeserializationError(ValueError):
    pass


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

def normalize_and_hash(obj: Any) -> tuple[Any, str]:
    canonical = canonical_json(obj)
    return json.loads(canonical), hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def spec_hash_for(spec: Any) -> str:
    return hashlib.sha256(canonical_json(spec).encode("utf-8")).hexdigest()


def encode_payload(value: Any) -> Any:
    if isinstance(value, Model):
        if value.pk is None:
            raise ValueError("Cannot serialize model instance without primary key")
        return {
            TYPE_MARKER: "model",
            "app_label": value._meta.app_label,
            "model": value._meta.model_name,
            "pk": str(value.pk),
        }
    if isinstance(value, timedelta):
        return {
            TYPE_MARKER: "timedelta",
            "days": value.days,
            "seconds": value.seconds,
            "microseconds": value.microseconds,
        }
    if isinstance(value, (list, tuple)):
        return [encode_payload(item) for item in value]
    if isinstance(value, dict):
        return {key: encode_payload(val) for key, val in value.items()}
    return value


def decode_payload(value: Any, *, using: str | None = None) -> Any:
    if isinstance(value, list):
        return [decode_payload(item, using=using) for item in value]
    if isinstance(value, dict):
        if TYPE_MARKER in value:
            type_name = value.get(TYPE_MARKER)
            if type_name == "timedelta":
                return timedelta(
                    days=value.get("days", 0),
                    seconds=value.get("seconds", 0),
                    microseconds=value.get("microseconds", 0),
                )
            if type_name == "model":
                app_label = value.get("app_label")
                model_name = value.get("model")
                pk = value.get("pk")
                if not app_label or not model_name:
                    raise DeserializationError("Invalid model reference payload")
                model_cls = apps.get_model(app_label, model_name)
                if model_cls is None:
                    raise DeserializationError(f"Unknown model {app_label}.{model_name}")
                manager = model_cls._default_manager
                if using:
                    manager = manager.using(using)
                try:
                    return manager.get(pk=pk)
                except model_cls.DoesNotExist as exc:
                    raise DeserializationError(
                        f"Model instance {app_label}.{model_name}({pk}) not found"
                    ) from exc
            raise DeserializationError(f"Unknown payload type: {type_name}")
        return {key: decode_payload(val, using=using) for key, val in value.items()}
    return value


def encode_args_kwargs(args: tuple, kwargs: dict) -> tuple[list, dict]:
    encoded_args = encode_payload(list(args))
    encoded_kwargs = encode_payload(dict(kwargs))
    return encoded_args, encoded_kwargs


def decode_args_kwargs(args: list, kwargs: dict, *, using: str | None = None) -> tuple[list, dict]:
    decoded_args = decode_payload(args, using=using)
    decoded_kwargs = decode_payload(kwargs, using=using)
    return decoded_args, decoded_kwargs

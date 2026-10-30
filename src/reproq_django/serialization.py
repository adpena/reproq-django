import hashlib
import json
from datetime import timedelta
from typing import Any

from django.apps import apps
from django.conf import settings
from django.core.exceptions import ValidationError
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
        allow_nan=False,
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
        database = value._state.db
        if value._state.adding or value.pk is None or not database:
            raise ValueError("Cannot serialize an unsaved model instance")
        if not isinstance(database, str) or database not in settings.DATABASES:
            raise ValueError("Model source database alias is not configured")
        return {
            TYPE_MARKER: "model",
            "app_label": value._meta.app_label,
            "model": value._meta.model_name,
            "database": database,
            "pk": encode_payload(value.pk),
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
        _string_keys(value, ValueError)
        encoded = {key: encode_payload(val) for key, val in value.items()}
        if TYPE_MARKER in value:
            return {TYPE_MARKER: "literal_dict", "items": encoded}
        return encoded
    return value


def _string_keys(value, error_class):
    if any(not isinstance(key, str) for key in value):
        raise error_class("Payload dictionaries require string keys; implicit JSON key coercion is unsupported")


def decode_payload(value: Any) -> Any:
    if isinstance(value, list):
        return [decode_payload(item) for item in value]
    if isinstance(value, dict):
        _string_keys(value, DeserializationError)
        if TYPE_MARKER in value:
            type_name = value.get(TYPE_MARKER)
            if type_name == "literal_dict":
                items = value.get("items")
                if set(value) != {TYPE_MARKER, "items"} or not isinstance(items, dict):
                    raise DeserializationError("Invalid literal dictionary envelope")
                _string_keys(items, DeserializationError)
                # Do not decode the items mapping as a control envelope: its
                # marker-named key is literal data, not an instruction.
                return {key: decode_payload(val) for key, val in items.items()}
            if type_name == "timedelta":
                fields = {name: value.get(name) for name in ("days", "seconds", "microseconds")}
                if set(value) != {TYPE_MARKER, *fields} or any(type(n) is not int for n in fields.values()):
                    raise DeserializationError("Invalid timedelta envelope")
                try:
                    return timedelta(**fields)
                except OverflowError as exc:
                    raise DeserializationError("Timedelta envelope exceeds the supported range") from exc
            if type_name == "model":
                app_label = value.get("app_label")
                model_name = value.get("model")
                database = value.get("database")
                if (set(value) != {TYPE_MARKER, "app_label", "model", "database", "pk"}
                        or not isinstance(app_label, str) or not app_label
                        or not isinstance(model_name, str) or not model_name
                        or not isinstance(database, str) or database not in settings.DATABASES
                        or value["pk"] is None):
                    raise DeserializationError("Invalid model reference payload")
                try:
                    model_cls = apps.get_model(app_label, model_name)
                except LookupError as exc:
                    raise DeserializationError("Unknown model reference") from exc
                if model_cls is None:
                    raise DeserializationError("Unknown model reference")
                pk = decode_payload(value["pk"])
                try:
                    return model_cls._default_manager.using(database).get(pk=pk)
                except (model_cls.DoesNotExist, ValidationError, ValueError, TypeError) as exc:
                    raise DeserializationError("Referenced model instance could not be loaded") from exc
            raise DeserializationError("Unknown typed payload marker")
        return {key: decode_payload(val) for key, val in value.items()}
    return value


def encode_args_kwargs(args: tuple, kwargs: dict) -> tuple[list, dict]:
    if not isinstance(args, (tuple, list)) or not isinstance(kwargs, dict):
        raise ValueError("Task arguments require an array and a keyword mapping")
    _string_keys(kwargs, ValueError)
    return ([encode_payload(value) for value in args],
            {key: encode_payload(value) for key, value in kwargs.items()})


def decode_args_kwargs(args: list, kwargs: dict) -> tuple[list, dict]:
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise DeserializationError("Task arguments require an array and a keyword mapping")
    _string_keys(kwargs, DeserializationError)
    return ([decode_payload(value) for value in args],
            {key: decode_payload(value) for key, value in kwargs.items()})

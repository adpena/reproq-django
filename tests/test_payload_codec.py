"""Source-aware model references and unambiguous literal JSON task data."""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.conf import settings
from django.db import connections, models
from django.db.backends.base.base import BaseDatabaseWrapper
from django.test.utils import isolate_apps

from reproq_django import serialization
from reproq_django.models import QueueControl
from reproq_django.serialization import (
    TYPE_MARKER,
    DeserializationError,
    decode_args_kwargs,
    decode_payload,
    encode_args_kwargs,
    encode_payload,
    normalize_json,
    spec_hash_for,
)


@pytest.fixture
def model_sources():
    aliases = ("owned_codec_source", "owned_codec_queue")
    for alias in aliases:
        assert alias not in settings.DATABASES
        config = {**connections["default"].settings_dict, "NAME": ":memory:"}
        settings.DATABASES[alias] = config
        connections.databases[alias] = config
        with connections[alias].schema_editor() as editor:
            editor.create_model(QueueControl)
    try:
        yield aliases
    finally:
        for alias in aliases:
            connection = connections[alias]
            # Django deliberately ignores close() for in-memory SQLite. These
            # two connections are owned solely by this fixture.
            if connection.connection is not None:
                connection.connection.close()
            del connections[alias]
            settings.DATABASES.pop(alias, None)
            connections.databases.pop(alias, None)


def wire_roundtrip(args, kwargs):
    args, kwargs = encode_args_kwargs(args, kwargs)
    wire = normalize_json({"args": args, "kwargs": kwargs})
    return decode_args_kwargs(wire["args"], wire["kwargs"])


def forbid_database(*args, **kwargs):
    pytest.fail("A literal, malformed or ambiguous payload attempted database access")


def test_model_references_retain_actual_source_even_when_queue_has_same_pk(model_sources):
    source, queue = model_sources
    business = QueueControl.objects.using(source).create(queue_name="same-key", reason="business")
    different = QueueControl.objects.using(queue).create(queue_name="same-key", reason="different")
    args, kwargs = wire_roundtrip((business,), {"other": different})
    assert (args[0].pk, args[0]._state.db, args[0].reason) == ("same-key", source, "business")
    assert (kwargs["other"].pk, kwargs["other"]._state.db, kwargs["other"].reason) == (
        "same-key", queue, "different",
    )
    assert encode_payload(business)["database"] == source
    assert spec_hash_for(encode_payload(business)) != spec_hash_for(encode_payload(different))


def test_deleted_source_record_does_not_fall_back_to_another_database(model_sources):
    source, queue = model_sources
    original = QueueControl.objects.using(source).create(queue_name="same-key", reason="source")
    QueueControl.objects.using(queue).create(queue_name="same-key", reason="different")
    marker = encode_payload(original)
    original.delete(using=source)
    with pytest.raises(DeserializationError, match="could not be loaded"):
        decode_payload(marker)


def test_unsaved_preassigned_primary_key_is_not_a_saved_reference(monkeypatch):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    obj = QueueControl(queue_name="already-assigned")
    assert obj.pk is not None and obj._state.adding
    with pytest.raises(ValueError, match="unsaved"):
        encode_payload(obj)


@pytest.mark.parametrize("change", [
    {"database": None}, {"database": "not-configured"}, {"pk": None},
    {"app_label": []}, {"model": None}, {"unknown": "field"},
])
def test_ambiguous_or_malformed_model_markers_never_query_a_database(monkeypatch, change):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    marker = {TYPE_MARKER: "model", "app_label": "reproq_django", "model": "queuecontrol",
              "pk": "key", "database": "default", **change}
    with pytest.raises(DeserializationError):
        decode_payload(marker)


def test_legacy_model_marker_without_source_alias_fails_without_guessing(monkeypatch):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    with pytest.raises(DeserializationError, match="model reference"):
        decode_payload({TYPE_MARKER: "model", "app_label": "reproq_django",
                        "model": "queuecontrol", "pk": "key"})
    with pytest.raises(TypeError):
        decode_args_kwargs([], {}, using="default")


def test_invalid_typed_primary_key_fails_before_database_lookup(monkeypatch):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    with pytest.raises(DeserializationError, match="could not be loaded"):
        decode_payload({TYPE_MARKER: "model", "app_label": "reproq_django",
                        "model": "workflowrun", "pk": "not-a-uuid", "database": "default"})


def test_literal_reserved_markers_nested_data_and_keyword_names_do_not_become_instructions(monkeypatch):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    literal = {TYPE_MARKER: "model", "app_label": "auth", "model": "user",
               "database": "default", "pk": 1,
               "nested": {TYPE_MARKER: "timedelta", "seconds": 17},
               "zero": 0, "false": False, "null": None}
    exact_model_shape = {TYPE_MARKER: "model", "app_label": "auth", "model": "user",
                         "database": "default", "pk": 1}
    nested = {TYPE_MARKER: "literal_dict", "items": {TYPE_MARKER: "unknown"}}
    args, kwargs = wire_roundtrip((exact_model_shape, literal, [nested]),
                                 {TYPE_MARKER: "model", "seconds": 17, "data": literal})
    assert args == [exact_model_shape, literal, [nested]]
    assert kwargs == {TYPE_MARKER: "model", "seconds": 17, "data": literal}


@pytest.mark.parametrize("value", [
    {1: "integer"}, {"nested": {None: "none"}}, {"nested": {1: "number", "1": "text"}},
])
def test_nonstring_json_keys_fail_instead_of_colliding_or_changing_type(value):
    with pytest.raises(ValueError, match="string keys"):
        encode_payload(value)
    with pytest.raises(DeserializationError, match="string keys"):
        decode_payload(value)


@pytest.mark.parametrize("value", [timedelta(), timedelta.max, timedelta.min,
                                    timedelta(days=-2, seconds=1, microseconds=999999)])
def test_supported_timedeltas_keep_sign_days_and_full_microseconds(value):
    assert wire_roundtrip((value,), {"duration": value}) == ([value], {"duration": value})


@pytest.mark.parametrize("payload", [
    {TYPE_MARKER: "literal_dict", "items": []},
    {TYPE_MARKER: "literal_dict", "items": {}, "extra": True},
    {TYPE_MARKER: "timedelta", "days": 0, "seconds": True, "microseconds": 0},
    {TYPE_MARKER: "timedelta", "days": 10**20, "seconds": 0, "microseconds": 0},
    {TYPE_MARKER: "unknown"},
])
def test_invalid_control_envelopes_fail_closed_without_database(monkeypatch, payload):
    monkeypatch.setattr(BaseDatabaseWrapper, "ensure_connection", forbid_database)
    with pytest.raises(DeserializationError):
        decode_payload(payload)


def test_native_composite_primary_key_keeps_typed_components_and_source(model_sources):
    source, queue = model_sources
    with isolate_apps("django.contrib.contenttypes") as registry:
        class OwnedCodecPair(models.Model):
            pk = models.CompositePrimaryKey("scope", "number")
            scope = models.CharField(max_length=30)
            number = models.IntegerField()
            value = models.CharField(max_length=30)

            class Meta:
                app_label = "contenttypes"
                db_table = "owned_codec_pair"

        for alias in (source, queue):
            with connections[alias].schema_editor() as editor:
                editor.create_model(OwnedCodecPair)
        original = OwnedCodecPair.objects.using(source).create(scope="district", number=0, value="source")
        OwnedCodecPair.objects.using(queue).create(scope="district", number=0, value="other")
        with patch.object(serialization, "apps", registry):
            encoded = encode_payload(original)
            assert encoded["pk"] == ["district", 0]
            decoded = decode_payload(normalize_json(encoded))
        assert (decoded.pk, decoded._state.db, decoded.value) == (("district", 0), source, "source")

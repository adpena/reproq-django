"""The shared inspector validates identifiers before touching a database."""

from types import SimpleNamespace

import pytest

from reproq_django.schema_inspection import (
    SchemaInspectionError,
    catalog_sha256_v1,
    require_public_relations_v1,
)


@pytest.mark.parametrize("relations", [None, "table", b"table", [], [""], [None], [{}], ["a\x00b"]])
def test_invalid_relation_identifiers_fail_before_database_access(relations):
    connection = SimpleNamespace(vendor="postgresql")
    with pytest.raises(SchemaInspectionError):
        require_public_relations_v1(connection, relations=relations)


def test_schema_digest_preserves_original_v1_json_bytes():
    import hashlib
    import json

    catalog = {"columns": [["résumé", "value", None, True, 64]], "tables": []}
    expected = hashlib.sha256(json.dumps(catalog, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert catalog_sha256_v1(catalog) == expected

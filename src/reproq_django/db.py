from __future__ import annotations

import fnmatch
from typing import Tuple

from django.conf import settings

DEFAULT_DB_ALIAS = "default"


def default_db_alias() -> str:
    return getattr(settings, "REPROQ_DEFAULT_DB_ALIAS", DEFAULT_DB_ALIAS)


def queue_database_map() -> dict[str, str]:
    mapping = getattr(settings, "REPROQ_QUEUE_DATABASES", None)
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ValueError("REPROQ_QUEUE_DATABASES must be a dict of queue->db_alias")
    return mapping


def resolve_queue_db(queue_name: str | None) -> str:
    if not queue_name:
        queue_name = "default"
    mapping = queue_database_map()
    if queue_name in mapping:
        return mapping[queue_name]
    for key, alias in mapping.items():
        if _is_glob(key) and fnmatch.fnmatchcase(queue_name, key):
            return alias
    return default_db_alias()


def queue_db_aliases() -> list[str]:
    aliases = {default_db_alias()}
    for alias in queue_database_map().values():
        aliases.add(alias)
    return sorted(aliases)


def should_prefix_result_ids() -> bool:
    override = getattr(settings, "REPROQ_RESULT_ID_WITH_ALIAS", None)
    if override is not None:
        return bool(override)
    return len(queue_db_aliases()) > 1


def format_result_id(result_id: int | str, db_alias: str, include_alias: bool | None = None) -> str:
    if include_alias is None:
        include_alias = should_prefix_result_ids()
    result_str = str(result_id)
    if include_alias and db_alias:
        return f"{db_alias}:{result_str}"
    return result_str


def parse_result_id(value: int | str) -> Tuple[str, str]:
    if isinstance(value, int):
        return default_db_alias(), str(value)
    raw = str(value)
    if ":" in raw:
        alias, result_id = raw.split(":", 1)
        if alias in queue_db_aliases() or alias in getattr(settings, "DATABASES", {}):
            return alias, result_id
    return default_db_alias(), raw


def _is_glob(value: str) -> bool:
    return any(char in value for char in ("*", "?", "["))

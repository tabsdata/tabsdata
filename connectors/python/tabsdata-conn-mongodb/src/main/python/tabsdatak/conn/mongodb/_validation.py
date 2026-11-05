#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
from typing import Any

from pydantic import AfterValidator

import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, distinct_names_validate
from tabsdatak.conn.mongodb.error import MongoDBErrorCode

# --- Collection ------------------------------------------------------


_COLLECTION_MAX = 235  # leaves room for db.<sep>.collection within 255


def _within_utf8_bytes(value: str, limit: int) -> bool:
    return len(value.encode("utf-8")) <= limit


COLLECTION_NAME_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(MongoDBErrorCode.MONGODB_4, min_len=1),
    _api.Rule(
        MongoDBErrorCode.MONGODB_10,
        lambda value: _within_utf8_bytes(value, _COLLECTION_MAX),
        lambda value: {"max": _COLLECTION_MAX, "value": value},
    ),
    _api.Rule.forbids("\x00", MongoDBErrorCode.MONGODB_11),
    _api.Rule.forbids("$", MongoDBErrorCode.MONGODB_12),
    _api.Rule.check(
        lambda value: not value.startswith("system."), MongoDBErrorCode.MONGODB_13
    ),
)


# --- Field name ------------------------------------------------------


FIELD_NAME_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(MongoDBErrorCode.MONGODB_5, min_len=1),
    _api.Rule.forbids("\x00", MongoDBErrorCode.MONGODB_14),
    _api.Rule.forbids(".", MongoDBErrorCode.MONGODB_15),
    _api.Rule.check(
        lambda value: not value.startswith("$"), MongoDBErrorCode.MONGODB_16
    ),
)


# --- URI -------------------------------------------------------------

URI_PATTERN = r"(?i)mongodb(?:\+srv)?://.*"

URI_VALIDATION = _api.ValueConstraints(
    _api.Rule.matches(URI_PATTERN, MongoDBErrorCode.MONGODB_6)
)


_DISTINCT_COLLECTIONS = distinct_names_validate(FqnDef(separator="."))


def _distinct_collections(values: list[tuple[str, Any]]) -> list[tuple[str, Any]]:
    _DISTINCT_COLLECTIONS.func([collection for collection, _ in values])
    return values


COLLECTIONS_DISTINCT_VALIDATION = AfterValidator(_distinct_collections)

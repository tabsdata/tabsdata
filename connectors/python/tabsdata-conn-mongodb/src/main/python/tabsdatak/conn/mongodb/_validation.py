from __future__ import annotations

import re

from pydantic import AfterValidator

from tabsdatak.conn.mongodb.error import MongodbErrorCode

# --- Database name --------------------------------------------------


_DB_NAME_MAX_BYTES = 63
_DB_FORBIDDEN = set('/\\."$ *<>:|?')


def _validate_database_name(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_3.exception()
    if len(value.encode("utf-8")) > _DB_NAME_MAX_BYTES:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_7.exception(max=_DB_NAME_MAX_BYTES, value=value)
    if "\x00" in value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_8.exception(value=value)
    bad = sorted(set(value) & _DB_FORBIDDEN)
    if bad:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_9.exception(bad=bad, value=value)
    return value


DATABASE_NAME_VALIDATOR = AfterValidator(_validate_database_name)


# --- Collection ------------------------------------------------------


_COLLECTION_MAX = 235  # leaves room for db.<sep>.collection within 255


def _validate_collection_name(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_4.exception()
    if len(value.encode("utf-8")) > _COLLECTION_MAX:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_10.exception(max=_COLLECTION_MAX, value=value)
    if "\x00" in value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_11.exception(value=value)
    if "$" in value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_12.exception(value=value)
    if value.startswith("system."):
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_13.exception(value=value)
    return value


COLLECTION_NAME_VALIDATOR = AfterValidator(_validate_collection_name)


# --- Field name ------------------------------------------------------


def _validate_field_name(value: str) -> str:
    if not value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_5.exception()
    if "\x00" in value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_14.exception(value=value)
    if "." in value:
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_15.exception(value=value)
    if value.startswith("$"):
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_16.exception(value=value)
    return value


FIELD_NAME_VALIDATOR = AfterValidator(_validate_field_name)


# --- URI -------------------------------------------------------------


_URI_RE = re.compile(r"mongodb(?:\+srv)?://.*", re.IGNORECASE)


def _validate_uri(value: str) -> str:
    if not _URI_RE.fullmatch(value):
        # noinspection PyProtectedMember
        raise MongodbErrorCode.MONGODB_6.exception(value=value)
    return value


URI_VALIDATOR = AfterValidator(_validate_uri)

# Pure-regex equivalent for use as ``value_pattern``.
URI_PATTERN = r"(?i)mongodb(?:\+srv)?://.*"

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class MongoDBValidateException(ConnException):
    """Cross-field validation failure for the MongoDB connector."""


class MongoDBRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the MongoDB connector."""


class MongodbErrorCode(ErrorCode):
    MONGODB_1 = ErrorDef(
        ConnInitException,
        "MongoDBDestConn validation failed",
    )  # MongoDBDestConn
    MONGODB_2 = ErrorDef(
        ConnInitException,
        "MongoDBDest validation failed",
    )  # MongoDBDest
    MONGODB_3 = ErrorDef(
        ConnInitException,
        "MongoDB database name must be non-empty",
    )  # _validate_database_name: empty
    MONGODB_4 = ErrorDef(
        ConnInitException,
        "MongoDB collection name must be non-empty",
    )  # _validate_collection_name: empty
    MONGODB_5 = ErrorDef(
        ConnInitException,
        "MongoDB field name must be non-empty",
    )  # _validate_field_name: empty
    MONGODB_6 = ErrorDef(
        ConnInitException,
        "MongoDB URI must start with mongodb:// or mongodb+srv://: {value!r}",
    )  # _validate_uri
    MONGODB_7 = ErrorDef(
        ConnInitException,
        "MongoDB database name exceeds {max} bytes: {value!r}",
    )  # _validate_database_name: too long
    MONGODB_8 = ErrorDef(
        ConnInitException,
        "MongoDB database name may not contain null: {value!r}",
    )  # _validate_database_name: null byte
    MONGODB_9 = ErrorDef(
        ConnInitException,
        "MongoDB database name contains forbidden chars {bad!r}: {value!r}",
    )  # _validate_database_name: forbidden char
    MONGODB_10 = ErrorDef(
        ConnInitException,
        "MongoDB collection name exceeds {max} bytes: {value!r}",
    )  # _validate_collection_name: too long
    MONGODB_11 = ErrorDef(
        ConnInitException,
        "MongoDB collection name may not contain null: {value!r}",
    )  # _validate_collection_name: null byte
    MONGODB_12 = ErrorDef(
        ConnInitException,
        "MongoDB collection name may not contain '$': {value!r}",
    )  # _validate_collection_name: dollar
    MONGODB_13 = ErrorDef(
        ConnInitException,
        "MongoDB collection name may not start with 'system.': {value!r}",
    )  # _validate_collection_name: system. prefix
    MONGODB_14 = ErrorDef(
        ConnInitException,
        "MongoDB field name may not contain null: {value!r}",
    )  # _validate_field_name: null byte
    MONGODB_15 = ErrorDef(
        ConnInitException,
        "MongoDB field name may not contain '.': {value!r}",
    )  # _validate_field_name: dot
    MONGODB_16 = ErrorDef(
        ConnInitException,
        "MongoDB field name may not start with '$': {value!r}",
    )  # _validate_field_name: dollar prefix
    MONGODB_17 = ErrorDef(
        MongoDBValidateException,
        "MongoDBDest.docs_per_trx must be > 0, got {docs_per_trx}",
    )  # MongoDBDest.validate(): docs_per_trx must be positive
    MONGODB_18 = ErrorDef(
        MongoDBRuntimeException,
        "MongoDBDest collections/slot count mismatch "
        "(collections={collections}, slots={slots})",
    )  # _MongoDBDestPlugin.write_out: defensive recheck
    MONGODB_19 = ErrorDef(
        MongoDBRuntimeException,
        "MongoDB destination connection failed",
    )  # _MongoDBDestPlugin.write_out: client connect / ping failure
    MONGODB_20 = ErrorDef(
        MongoDBRuntimeException,
        "MongoDB write failed (collection={collection!r})",
    )  # _MongoDBDestPlugin.write_out: bulk_write failure
    MONGODB_21 = ErrorDef(
        MongoDBRuntimeException,
        "MongoDBDest id_field {id_field!r} is missing from a document"
        " in collection {collection!r}",
    )  # _MongoDBDestPlugin.write_out: id_field absent on a document
    MONGODB_22 = ErrorDef(
        MongoDBRuntimeException,
        "MongoDBDest collection name {value!r} must be a fully-qualified"
        " <database>.<collection> when used as a write target",
    )  # _MongoDBDestPlugin.write_out: collection name not FQN-shaped
    MONGODB_23 = ErrorDef(MongoDBRuntimeException, "Cannot Create MongoIndex")
    MONGODB_24 = ErrorDef(MongoDBRuntimeException, "Cannot Create MongoValidator")
    MONGODB_25 = ErrorDef(MongoDBRuntimeException, "Cannot Create MongoDBEntity")
    MONGODB_26 = ErrorDef(MongoDBRuntimeException, "Cannot Create MongoDBConnMetaModel")

#
# Copyright 2026 Tabsdata Inc.
#

"""MongoDB connector -- public API.

Two classes, imported from `tabsdatak.conn.mongodb`:

* `MongoDBDestConn` -- the connection: a `uri` (without
  credentials) plus a `UserPassword` the plugin splices into the URI
  at write time. Registered on a collection, not passed to a decorator.
* `MongoDBDest` -- the `@subscriber` destination: a list of
  `(collection, id_field)` pairs, one output slot per collection.

There is no MongoDB source today. The connection (with credentials) is
resolved from the function's collection at run time; a flow function
only references `MongoDBDest`.

Examples:
    .. code-block:: python

        @subscriber(
            destination=MongoDBDest(collections_with_ids=[("users", "id")]),
            input_tables=["mycoll/users"],
        )
        def publish(users: TableFrameSpec) -> TableFrameSpec:
            return users
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Dest, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import (
    CfgSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import _validation
from tabsdatak.conn.mongodb.error import MongodbErrorCode
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.URI_PATTERN)
]
CollectionSpec: TypeAlias = Annotated[str, _validation.COLLECTION_NAME_VALIDATOR]
ColumnNameSpec: TypeAlias = Annotated[str, _validation.FIELD_NAME_VALIDATOR]
CollectionsWithIdsSpec: TypeAlias = list[tuple[CollectionSpec, ColumnNameSpec | None]]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


# --- MongoDBDest.dest_cfg keys --------------------------------------
DestCfgKey: TypeAlias = Literal["mongodb.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_1, kw_only=True)
class MongoDBDestConn(Conn):
    """MongoDB connection used by `MongoDBDest`.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time.

    Args:
        uri: MongoDB connection URI WITHOUT credentials, e.g.
            `mongodb://host:27017/?...`. Must start with
            `mongodb://` or `mongodb+srv://`. The plugin splices in
            the user / password from `credentials` at write time.
            Accepts a plain string or a `Secret`.
        credentials: `UserPassword` for the connection.
        conn_cfg: Optional connector config; `pymongo.*` keys are
            forwarded to `pymongo.MongoClient` (see `CfgSpec`).
    """

    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None

    def _uri(self) -> str:
        return _convert(self.uri, str)


# --- Destination -----------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_2, kw_only=True)
class MongoDBDest(Dest):
    """MongoDB destination for `@subscriber` -- one slot per
    collection.

    :categories: destination

    Args:
        collections_with_ids: List of `(collection, id_field)`
            pairs, one write slot per entry. `collection` is the
            target collection, optionally qualified as
            `<database>.<collection>`; a bare `<collection>`
            targets the database named in the connection URI (a name up
            to 235 bytes, no NUL or `$`, not starting with
            `system.`). `id_field` is the document field used as
            the unique id, e.g. `("mydb.users", "user_id")`; pass
            `None` (e.g. `("mydb.logs", None)`) to let MongoDB
            auto-generate `_id`. An `id_field` may not be empty,
            contain `.` or NUL, or start with `$`.
        if_collection_exists: `"append"` (default) leaves existing
            documents in place; `"replace"` drops the collection
            first.
        use_trxs: When true, each batch of `docs_per_trx` documents
            is wrapped in a MongoDB transaction (requires a replica
            set).
        docs_per_trx: Max documents per write batch / transaction
            (default 1000; must be greater than 0).
        maintain_order: When true, documents are inserted in input
            order (slower but deterministic server-side ordering).
        update_existing: When true, documents whose id already exists
            are upserted (`UpdateOne` with `upsert=True`); when
            false, `InsertOne` is used and a collision aborts the
            batch. Ignored when `id_field` is `None` (auto-generated
            ids never collide).
        fail_on_duplicate_key: When true, a duplicate-key error is
            re-raised; when false it is logged and the batch continues.
        log_intermediate_files: When true, each intermediate JSONL
            chunk written to the work dir is logged before upload --
            useful for debugging large writes.
        dest_cfg: Optional connector config over the single key
            `mongodb.logging_level`. Note: `pymongo.*` options are
            read from the connection's `conn_cfg`, not here.

    Examples:
        Append documents, letting MongoDB auto-generate `_id` (the
        id field is `None`):

        .. code-block:: python

            @subscriber(
                destination=MongoDBDest(
                    collections_with_ids=[("cap_append", None)],
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def publish(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Upsert on a natural key -- `code` becomes each document's
        `_id`, so re-runs update the same documents instead of
        inserting duplicates:

        .. code-block:: python

            @subscriber(
                destination=MongoDBDest(
                    collections_with_ids=[("cap_upsert", "code")],
                    update_existing=True,
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def upsert(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    collections_with_ids: CollectionsWithIdsSpec
    if_collection_exists: IfTableExistsSpec = "append"
    use_trxs: bool = False
    docs_per_trx: int = 1000
    maintain_order: bool = False
    update_existing: bool = True
    fail_on_duplicate_key: bool = True
    log_intermediate_files: bool = False
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Validate that `docs_per_trx` is positive.

        Called by the framework; raises `MongoDBValidateException`.
        """
        if self.docs_per_trx <= 0:
            raise MongodbErrorCode.MONGODB_17.exception(
                docs_per_trx=self.docs_per_trx,
            )

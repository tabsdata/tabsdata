"""MongoDB connector — public API.

* :class:`MongoDBDestConn` -- per-cluster MongoDB connection
  (URI without credentials + a :class:`UserPassword` that the
  plugin splices into the URI at write time).
* :class:`MongoDBDest` -- destination: list of
  ``(<db>.<collection>, <id_field>|None)`` tuples; one parquet
  slot per collection. ``id_field=None`` does insert-only;
  ``id_field="user_id"`` does upsert-by-id.

No ``MongoDBSrc`` today.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Dest, Var, NamedConn, Secret

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import CfgSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import _validation
from tabsdatak.conn.mongodb.error import MongodbErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Var, _api.VarValueConstraints(pattern=_validation.URI_PATTERN)],
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
    """MongoDB connection.

    :param uri: MongoDB connection URI WITHOUT credentials
        (``mongodb://host:port/?...``). The plugin splices in
        the user / password from ``credentials`` at write time.
    :param credentials: :class:`UserPassword`.
    :param conn_cfg: Optional driver-side config dict.
    """

    __kind__ = "mongoDBDestConn"

    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MongoDBDestConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_2, kw_only=True)
class MongoDBDest(Dest):
    """MongoDB destination — one parquet slot per collection.

    :param conn: :class:`MongoDBDestConn` (or :class:`NamedConn`).
    :param collections_with_ids: List of
        ``(collection_name, id_field)`` tuples; one slot per
        entry. ``collection_name`` is the destination collection
        and ``id_field`` is the document field used as the unique
        id (e.g. ``("users", "user_id")``). Pass ``id_field=None``
        (e.g. ``("logs", None)``) to let MongoDB auto-generate
        ``_id``.
    :param if_collection_exists: ``"append"`` (default) leaves
        existing documents alone; ``"replace"`` drops the
        collection first.
    :param use_trxs: When ``True``, each batch of ``docs_per_trx``
        documents is wrapped in a MongoDB transaction. Requires the
        server to be configured with a replica set.
    :param docs_per_trx: Max documents per write batch /
        transaction (default 1000). Must be > 0.
    :param maintain_order: When ``True``, documents are inserted in
        the order they appear in the input (slower but deterministic
        ordering on the server).
    :param update_existing: When ``True``, existing documents with
        the same id are upserted (``UpdateOne`` with
        ``upsert=True``); when ``False``, ``InsertOne`` is used and
        a collision aborts the batch. Ignored when ``id_field`` is
        ``None`` (auto-generated ids never collide).
    :param fail_on_duplicate_key: When ``True``, a duplicate-key
        error from MongoDB is re-raised. When ``False`` the error is
        logged and the batch continues.
    :param log_intermediate_files: When ``True``, each intermediate
        JSONL chunk written to the work dir is logged before upload
        -- useful for debugging large writes.
    :param dest_cfg: Optional ``Mapping`` over the single key
        ``"mongodb.logging_level"`` (int). Note: ``pymongo.*``
        options are read from ``conn.conn_cfg`` (not ``dest_cfg``)
        and forwarded to ``pymongo.MongoClient``.
    """

    conn: ConnSpec
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
        if self.docs_per_trx <= 0:
            raise MongodbErrorCode.MONGODB_17.exception(
                docs_per_trx=self.docs_per_trx,
            )

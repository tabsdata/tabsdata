"""MongoDB connector — public API.

* :class:`MongoDBConn` -- per-cluster MongoDB connection
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
from tabsdatak.api import Conn, Dest, Env, NamedConn, Secret

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
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
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
class MongoDBConn(Conn):
    """MongoDB connection.

    :param uri: MongoDB connection URI WITHOUT credentials
        (``mongodb://host:port/?...``). The plugin splices in
        the user / password from ``credentials`` at write time.
    :param credentials: :class:`UserPassword`.
    :param conn_cfg: Optional driver-side config dict.
    """

    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MongoDBConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_2, kw_only=True)
class MongoDBDest(Dest):
    """MongoDB destination.

    Fields:
      conn: MongoDB connection (or a NamedConn ref).
      collections_with_ids: list of ``(collection, id_field)`` tuples
        identifying each target collection and the document field to
        use as the unique id. ``id_field=None`` lets MongoDB
        autogenerate the ``_id``.
      if_collection_exists: ``"append"`` (default) leaves existing
        documents alone; ``"replace"`` drops the collection first.
      use_trxs: when True, each batch of ``docs_per_trx`` documents is
        wrapped in a MongoDB transaction. Requires the server to be
        configured with a replica set.
      docs_per_trx: maximum documents per write batch / transaction.
      maintain_order: when True, documents are inserted in the order
        they appear in the input.
      update_existing: when True, existing documents with the same id
        are upserted (UpdateOne with upsert=True); when False the
        insert proceeds with InsertOne and a collision aborts the
        batch. Ignored when ``id_field`` is None (auto-generated ids
        never collide).
      fail_on_duplicate_key: when True, a duplicate-key error from
        MongoDB is re-raised. When False the error is logged and the
        batch continues.
      log_intermediate_files: when True, each intermediate JSONL
        chunk is logged as it is persisted. Useful for debugging
        large writes.
      dest_cfg: typed plugin-level config (see ``DestCfgKey``).
        Currently the only key is ``"mongodb.logging_level"`` (int).
        Note: ``pymongo.*`` options are read from ``conn.conn_cfg``
        (not ``dest_cfg``) and forwarded to ``pymongo.MongoClient``.
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

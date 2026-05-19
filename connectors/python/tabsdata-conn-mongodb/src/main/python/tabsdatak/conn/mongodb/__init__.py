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
DestCfgKey: TypeAlias = Literal[
    "mongodb.logging_level",
]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Connection ------------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_1, kw_only=True)
class MongoDBConn(Conn):
    uri: UriSpec
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, MongoDBConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(MongodbErrorCode.MONGODB_2, kw_only=True)
class MongoDBDest(Dest):
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

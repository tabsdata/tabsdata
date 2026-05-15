from __future__ import annotations

from typing import Annotated, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Dest, Env, NamedConn, Secret
from tabsdatak.conn.common.types import CfgSpec, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.mongodb import _validation
from tabsdatak.conn.mongodb.error import MongodbErrorCode

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = UserPassword

UriSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URI_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URI_PATTERN)],
]
CollectionSpec: TypeAlias = Annotated[str, _validation.COLLECTION_NAME_VALIDATOR]
ColumnNameSpec: TypeAlias = Annotated[str, _validation.FIELD_NAME_VALIDATOR]
CollectionsWithIdsSpec: TypeAlias = list[tuple[CollectionSpec, ColumnNameSpec | None]]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]


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
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None

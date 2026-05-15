from __future__ import annotations

from typing import Annotated, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Conn, Dest, Env, NamedConn, Secret

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _validation
from tabsdatak.conn.bigquery.error import BigqueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import CfgSpec, GCPJson, GCSLocation

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = GCPJson

# Project ID and Dataset ID regexes (extracted from the validators).
_PROJECT_PATTERN = r"[a-z][a-z0-9-]{4,28}[a-z0-9]"
_DATASET_PATTERN = r"[A-Za-z0-9_]{1,1024}"


ProjectSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_PROJECT_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_PROJECT_PATTERN)],
]

DatasetSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_DATASET_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_DATASET_PATTERN)],
]

TableIdNameSpec: TypeAlias = Annotated[str, _validation.TABLE_ID_VALIDATOR]
BigQueryTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_FQN_VALIDATOR]
BigQueryTableNamesSpec: TypeAlias = list[BigQueryTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]


# --- Connection ------------------------------------------------------


@_api.dataclass(BigqueryErrorCode.BIGQUERY_1, kw_only=True)
class BigQueryConn(Conn):
    staging: GCSLocation
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    dataset: DatasetSpec | None = None
    conn_cfg: CfgSpec | None = None

    def validate(self) -> None:
        if self.dataset is not None and self.project is None:
            # noinspection PyProtectedMember
            raise BigqueryErrorCode.BIGQUERY_2.exception(dataset=self.dataset)


ConnSpec: TypeAlias = Union[NamedConn, BigQueryConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(BigqueryErrorCode.BIGQUERY_3, kw_only=True)
class BigQueryDest(Dest):
    conn: ConnSpec
    tables: BigQueryTableNamesSpec | None = None
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None

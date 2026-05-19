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
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    GCPJson,
    GCSLocation,
)

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = GCPJson


ProjectSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.PROJECT_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.PROJECT_PATTERN)],
]

DatasetSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.DATASET_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.DATASET_PATTERN)],
]

BigQueryTableNameSpec: TypeAlias = Annotated[
    str, _validation.BIGQUERY_TABLE_FQN_VALIDATOR
]
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
    entity_whitelist: EntityWhitelistSpec | None = None
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
    dest_cfg: CfgSpec | None = None

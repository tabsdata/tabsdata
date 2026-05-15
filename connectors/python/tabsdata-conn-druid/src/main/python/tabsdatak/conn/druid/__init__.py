from __future__ import annotations

from typing import Annotated, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import (
    Conn,
    Dest,
    Env,
    NamedConn,
    Secret,
)
from tabsdatak.conn.common.types import CfgSpec, CloudLocation, UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.druid import _validation
from tabsdatak.conn.druid.error import DruidErrorCode

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = UserPassword

UrlSpec: TypeAlias = Union[
    Annotated[Secret, _api.SecretValueConstraints(pattern=_validation.URL_PATTERN)],
    Annotated[Env, _api.EnvValueConstraints(pattern=_validation.URL_PATTERN)],
]
DatasourceSpec: TypeAlias = Annotated[str, _validation.DATASOURCE_VALIDATOR]
DruidTableNameSpec: TypeAlias = DatasourceSpec
DruidTableNamesSpec: TypeAlias = list[DruidTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
# Druid's accepted segment-granularity tokens (subset, the most-used
# values; the engine accepts more but these cover the common cases).
SegmentGranularitySpec: TypeAlias = Literal[
    "ALL",
    "NONE",
    "SECOND",
    "MINUTE",
    "FIFTEEN_MINUTE",
    "THIRTY_MINUTE",
    "HOUR",
    "SIX_HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
]


# --- Connection ------------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_1, kw_only=True)
class DruidConn(Conn):
    overlord_url: UrlSpec
    staging: CloudLocation
    credentials: CredentialsSpec
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, DruidConn]


# --- Destination -----------------------------------------------------


@_api.dataclass(DruidErrorCode.DRUID_2, kw_only=True)
class DruidDest(Dest):
    conn: ConnSpec
    tables: DruidTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    time_column: str | None = None
    segment_granularity: SegmentGranularitySpec = "ALL"
    max_concurrent_subtasks: int = 1
    max_rows_per_segment: int = 5_000_000
    max_rows_in_memory: int = 1_000_000
    http_timeout_sec: int = 300
    # TODO: document valid keys for this cfg in the docstring above
    # (see MySQLSrc src_cfg for the pattern).
    dest_cfg: CfgSpec | None = None

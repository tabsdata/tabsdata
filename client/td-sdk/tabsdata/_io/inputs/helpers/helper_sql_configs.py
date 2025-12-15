#
# Copyright 2025 Tabs Data Inc.
#

from dataclasses import dataclass
from typing import Any

from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.inputs.helpers.helper_constants import _LoaderSpec
from tabsdata.tableframe.typing import DataType


@dataclass
class SQLConnConfig:
    uri: str
    credentials: UserPasswordCredentials | None
    enforce_connection_params: bool = True
    cx_src_configs_sql: dict = None
    cx_dst_configs_sql: dict = None


@dataclass
class SQLSourceConfig:
    conn: SQLConnConfig
    queries: list[str]
    initial_values: dict[str, Any] | None
    transactional: bool
    chunk_size: int
    loader: _LoaderSpec
    schema_overrides: list[dict[str, DataType]] | None

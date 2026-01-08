#
# Copyright 2025 Tabs Data Inc.
#

from typing import Literal, Union

from tabsdata.tableframe.typing import DataType

QueriesSpec = Union[str, list[str]]
SchemaOverrideSpec = Union[dict[str, DataType], list[dict[str, DataType]], None]

MySQLLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MariaDBLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
OracleLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
PostgresLoaderSpec = Literal["polars_sqlalchemy", "pandas"]

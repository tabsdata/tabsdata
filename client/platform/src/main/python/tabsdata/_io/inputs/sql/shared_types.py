#
# Copyright 2025 Tabsdata Inc.
#

from typing import Literal, Union

from tabsdata.tableframe.typing import DataType

QueriesSpec = Union[str, list[str]]
SchemaOverrideSpec = Union[dict[str, DataType], list[dict[str, DataType]], None]

# Loader specs are defined here to avoid potential circular import issues
# that arise when importing _td_validate_call from dataquality module into the SQL input
# files.
Db2SrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
OracleLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
OracleSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
PostgresSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MariaDBSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MySQLSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]

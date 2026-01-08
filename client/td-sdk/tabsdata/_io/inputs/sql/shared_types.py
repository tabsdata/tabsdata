#
# Copyright 2025 Tabs Data Inc.
#

from typing import Literal, Union

from tabsdata.tableframe.typing import DataType

QueriesSpec = Union[str, list[str]]
SchemaOverrideSpec = Union[dict[str, DataType], list[dict[str, DataType]], None]

# Adding loader specs here to potential circular import issues wrt imports from
# dataquality module in respective sql input files.
OracleLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
PostgresLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MariaDBLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MySQLLoaderSpec = Literal["polars_sqlalchemy", "pandas"]

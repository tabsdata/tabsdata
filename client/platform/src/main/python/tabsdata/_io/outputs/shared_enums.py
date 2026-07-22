#
# Copyright 2025 Tabsdata Inc.
#

from enum import Enum
from typing import Literal, TypeAlias

IfTableExistStrategySpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["none", "iceberg"]
SchemaStrategySpec: TypeAlias = Literal["update", "strict"]

# Not including 'pandas' in the loader spec since pl.write_database uses pandas.to_sql
# under the hood by default
MariaDBDestWriterSpec = Literal["polars_sqlalchemy"]
MySQLDestWriterSpec = Literal["polars_sqlalchemy"]
OracleDestWriterSpec = Literal["polars_sqlalchemy"]
PostgresDestWriterSpec = Literal["polars_sqlalchemy"]


class IfTableExistsStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    APPEND = "append"
    REPLACE = "replace"


class SchemaStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    UPDATE = "update"
    STRICT = "strict"

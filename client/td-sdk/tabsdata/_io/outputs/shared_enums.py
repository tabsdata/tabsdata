#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum
from typing import Literal, TypeAlias

IfTableExistStrategySpec: TypeAlias = Literal["append", "replace"]
SchemaStrategySpec: TypeAlias = Literal["update", "strict"]

# Not including 'pandas' in the loader spec since pl.write_database uses pandas.to_sql
# under the hood by default
MySQLDestWriterSpec = Literal["polars_sqlalchemy"]


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

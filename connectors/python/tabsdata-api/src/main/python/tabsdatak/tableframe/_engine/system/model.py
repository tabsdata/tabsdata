#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any, Callable

from tabsdatak.tableframe._engine.system.constants.columns.names import (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    TD_FILE_LINE_SYSTEM_COLUMN_NAME,
    TD_FILE_PATH_SYSTEM_COLUMN_NAME,
    TD_INDEX_SYSTEM_COLUMN_NAME,
    TD_ITEM_SYSTEM_COLUMN_NAME,
    TD_MAXIMUM_SYSTEM_COLUMN_NAME,
    TD_MINIMUM_SYSTEM_COLUMN_NAME,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
    TD_USER_FUNCTION_INPUT_SYSTEM_COLUMN_NAME,
    TD_USER_FUNCTION_OUTPUT_SYSTEM_COLUMN_NAME,
    TD_USER_FUNCTION_WORK_SYSTEM_COLUMN_NAME,
    TD_VERSION_SYSTEM_COLUMN_NAME,
)

if TYPE_CHECKING:
    import polars as pl
    from polars.datatypes import DataTypeClass

    SystemColumnDataType = pl.DataType | DataTypeClass
    SystemColumnDefault = pl.Expr | str | datetime | None


class Language(Enum):
    PYTHON = "python"
    RUST = "rust"


class Inception(Enum):
    PROPAGATE = "propagate"
    REGENERATE = "regenerate"


class SystemColumnCategory(Enum):
    STANDARD = "standard"
    OPTIONAL = "optional"
    CONNECTOR = "connector"
    VOLATILE = "volatile"
    EXTENDED = "extended"


class RowOperation(Enum):
    """Values are persisted in stored data; do not change them."""

    ROW = 0
    UNDEFINED = 1
    GROUP_MIN = 2
    GROUP_MAX = 3
    GROUP_SUM = 4
    GROUP_LEN = 5
    GROUP_COUNT = 6
    GROUP_MEAN = 7
    GROUP_MEDIAN = 8
    GROUP_UNIQUE = 9


class SysCol(StrEnum):
    VERSION_EXECUTION = TD_EXECUTION_SYSTEM_COLUMN_NAME
    VERSION_TRANSACTION = TD_TRANSACTION_SYSTEM_COLUMN_NAME
    VERSION_VERSION = TD_VERSION_SYSTEM_COLUMN_NAME
    VERSION_TIMESTAMP = TD_TIMESTAMP_SYSTEM_COLUMN_NAME
    ORIGIN_FILE_PATH = TD_FILE_PATH_SYSTEM_COLUMN_NAME
    ORIGIN_FILE_LINE = TD_FILE_LINE_SYSTEM_COLUMN_NAME


class VolatileColumns(StrEnum):
    INDEX = TD_INDEX_SYSTEM_COLUMN_NAME
    ITEM = TD_ITEM_SYSTEM_COLUMN_NAME
    MINIMUM = TD_MINIMUM_SYSTEM_COLUMN_NAME
    MAXIMUM = TD_MAXIMUM_SYSTEM_COLUMN_NAME
    USER_FUNCTION_INPUT = TD_USER_FUNCTION_INPUT_SYSTEM_COLUMN_NAME
    USER_FUNCTION_WORK = TD_USER_FUNCTION_WORK_SYSTEM_COLUMN_NAME
    USER_FUNCTION_OUTPUT = TD_USER_FUNCTION_OUTPUT_SYSTEM_COLUMN_NAME


@dataclass(slots=True, frozen=True, eq=True)
class SystemColumn:
    name: str
    category: SystemColumnCategory
    dtype: Callable[[], SystemColumnDataType]
    default: Callable[[], SystemColumnDefault]
    language: Language
    inception: Inception
    generator: Any = None
    aggregation: Any = None

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import TYPE_CHECKING

from tabsdatak.extensions.tableframe._engine.generators import (
    ConstantVersionGenerator,
    IdentifierGenerator,
)
from tabsdatak.tableframe._engine.constants.columns.names import (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    TD_FILE_LINE_SYSTEM_COLUMN_NAME,
    TD_FILE_PATH_SYSTEM_COLUMN_NAME,
    TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
    TD_OFFSET_SYSTEM_COLUMN_NAME,
    TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_TRANSACTION_SYSTEM_COLUMN_NAME,
    TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME,
    TD_VERSION_SYSTEM_COLUMN_NAME,
)
from tabsdatak.tableframe._engine.constants.empties import (
    EMPTY_EXECUTION,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)
from tabsdatak.tableframe._engine.model.columns import (
    Inception,
    Language,
    SystemColumn,
    SystemColumnCategory,
)

if TYPE_CHECKING:
    import polars as pl_

    from tabsdatak.tableframe._engine.model.columns import (
        SystemColumnDataType as SystemColumnDataType_,
    )


def dtype_string() -> SystemColumnDataType_:
    import polars as pl

    return pl.String


def dtype_timestamp() -> SystemColumnDataType_:
    from datetime import timezone

    import polars as pl

    return pl.Datetime(time_unit="us", time_zone=timezone.utc)


def default_empty_string() -> pl_.Expr:
    import polars as pl

    return pl.lit("", pl.String)


def no_default() -> None:
    return None


STANDARD: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=default_empty_string,
        language=Language.RUST,
        inception=Inception.REGENERATE,
        required=True,
        pristine=True,
        generator=IdentifierGenerator(),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_OFFSET_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
    SystemColumn(
        name=TD_EXECUTION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_EXECUTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(dtype_string, lambda: EMPTY_EXECUTION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_TRANSACTION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_TRANSACTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(dtype_string, lambda: EMPTY_TRANSACTION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_VERSION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_string,
        default=lambda: EMPTY_VERSION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(dtype_string, lambda: EMPTY_VERSION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=dtype_timestamp,
        default=lambda: EMPTY_TIMESTAMP,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(dtype_timestamp, lambda: EMPTY_TIMESTAMP),
        aggregation=None,
    ),
)

OPTIONAL: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
    SystemColumn(
        name=TD_USER_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
    SystemColumn(
        name=TD_VALUE_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_timestamp,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
    SystemColumn(
        name=TD_FUNCTION_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
)

CONNECTOR: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_FILE_PATH_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.CONNECTOR,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
    SystemColumn(
        name=TD_FILE_LINE_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.CONNECTOR,
        dtype=dtype_string,
        default=no_default,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
)

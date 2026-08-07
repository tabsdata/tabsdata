#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import timezone

from tabsdatak.extensions.tableframe._engine.column.generator import (
    ConstantVersionGenerator,
    IdentifierGenerator,
)
from tabsdatak.tableframe import datatypes
from tabsdatak.tableframe._engine.constant.column.names import (
    TD_EXECUTION_SYSTEM_COLUMN_NAME,
    # TD_FILE_LINE_SYSTEM_COLUMN_NAME,
    # TD_FILE_PATH_SYSTEM_COLUMN_NAME,
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
from tabsdatak.tableframe._engine.constant.emptiness import (
    EMPTY_EXECUTION,
    EMPTY_STRING,
    EMPTY_TIMESTAMP,
    EMPTY_TRANSACTION,
    EMPTY_VERSION,
)
from tabsdatak.tableframe._engine.model.column import SystemColumn
from tabsdatak.tableframe._engine.taxonomy.column import (
    Inception,
    Language,
    SystemColumnCategory,
)

DTYPE_STRING = datatypes.String
DTYPE_TIMESTAMP = datatypes.Datetime(time_unit="us", time_zone=timezone.utc)


STANDARD: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_IDENTIFIER_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=DTYPE_STRING,
        default=EMPTY_STRING,
        language=Language.RUST,
        inception=Inception.REGENERATE,
        required=True,
        pristine=True,
        generator=IdentifierGenerator(DTYPE_STRING, EMPTY_STRING),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_OFFSET_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=DTYPE_STRING,
        default=None,
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
        dtype=DTYPE_STRING,
        default=EMPTY_EXECUTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(DTYPE_STRING, EMPTY_EXECUTION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_TRANSACTION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=DTYPE_STRING,
        default=EMPTY_TRANSACTION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(DTYPE_STRING, EMPTY_TRANSACTION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_VERSION_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=DTYPE_STRING,
        default=EMPTY_VERSION,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(DTYPE_STRING, EMPTY_VERSION),
        aggregation=None,
    ),
    SystemColumn(
        name=TD_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.STANDARD,
        dtype=DTYPE_TIMESTAMP,
        default=EMPTY_TIMESTAMP,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=True,
        pristine=False,
        generator=ConstantVersionGenerator(DTYPE_TIMESTAMP, EMPTY_TIMESTAMP),
        aggregation=None,
    ),
)

OPTIONAL: tuple[SystemColumn, ...] = (
    SystemColumn(
        name=TD_SERVER_TIMESTAMP_SYSTEM_COLUMN_NAME,
        category=SystemColumnCategory.OPTIONAL,
        dtype=DTYPE_STRING,
        default=None,
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
        dtype=DTYPE_STRING,
        default=None,
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
        dtype=DTYPE_TIMESTAMP,
        default=None,
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
        dtype=DTYPE_STRING,
        default=None,
        language=Language.PYTHON,
        inception=Inception.PROPAGATE,
        required=False,
        pristine=False,
        generator=None,
        aggregation=None,
    ),
)

# Connector columns are no longer a fixed set. They are injected per input
# file from the metadata a source plugin attaches to each `TableFileInput`,
# so nothing is declared here and the registry resolves them by the `@td.`
# prefix instead. The former fixed declarations are kept for reference:
#
# CONNECTOR: tuple[SystemColumn, ...] = (
#     SystemColumn(
#         name=TD_FILE_PATH_SYSTEM_COLUMN_NAME,
#         category=SystemColumnCategory.CONNECTOR,
#         dtype=DTYPE_STRING,
#         default=None,
#         language=Language.PYTHON,
#         inception=Inception.PROPAGATE,
#         required=False,
#         pristine=False,
#         generator=None,
#         aggregation=None,
#     ),
#     SystemColumn(
#         name=TD_FILE_LINE_SYSTEM_COLUMN_NAME,
#         category=SystemColumnCategory.CONNECTOR,
#         dtype=DTYPE_STRING,
#         default=None,
#         language=Language.PYTHON,
#         inception=Inception.PROPAGATE,
#         required=False,
#         pristine=False,
#         generator=None,
#         aggregation=None,
#     ),
# )
CONNECTOR: tuple[SystemColumn, ...] = ()

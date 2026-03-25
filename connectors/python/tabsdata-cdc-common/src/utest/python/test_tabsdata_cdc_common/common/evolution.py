#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import polars as pl
import pytest


class SchemaChangeKind(str, Enum):
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    CHANGE_TYPE = "change_type"


@dataclass
class SchemaChange:
    kind: SchemaChangeKind
    alter_sql: str
    old_column: str | None = None
    new_column: str | None = None
    new_type_name: str | None = None
    new_polars_type: pl.DataType | None = None


N_SCHEMA_ITERATIONS = 3

ADD_COLUMN_TARGETS = [
    ("c_extra_0", "varchar(100)"),
    ("c_extra_1", "varchar(100)"),
    ("c_extra_2", "varchar(100)"),
]

DROP_COLUMN_TARGETS = [
    "c_bool",
    "c_double",
    "c_bigint",
]

RENAME_COLUMN_TARGETS = [
    ("c_varchar", "c_text"),
    ("c_date", "c_date_r"),
    ("c_datetime", "c_ts_r"),
]

CHANGE_TYPE_TARGETS = [
    ("c_int", pl.Int64),
    ("c_float", pl.Float64),
    ("c_decimal", pl.Decimal(20, 4)),
]


SINGLE_CHANGES = [
    pytest.param(
        [SchemaChangeKind.ADD_COLUMN],
        id="add_column",
    ),
    pytest.param(
        [SchemaChangeKind.DROP_COLUMN],
        id="drop_column",
    ),
    pytest.param(
        [SchemaChangeKind.RENAME_COLUMN],
        id="rename_column",
    ),
    pytest.param(
        [SchemaChangeKind.CHANGE_TYPE],
        id="change_type",
    ),
]

MULTI_CHANGES = [
    pytest.param(
        [SchemaChangeKind.ADD_COLUMN, SchemaChangeKind.DROP_COLUMN],
        id="add_drop",
    ),
    pytest.param(
        [SchemaChangeKind.ADD_COLUMN, SchemaChangeKind.RENAME_COLUMN],
        id="add_rename",
    ),
]

ALL_CHANGES = [
    pytest.param(
        [
            SchemaChangeKind.ADD_COLUMN,
            SchemaChangeKind.DROP_COLUMN,
            SchemaChangeKind.RENAME_COLUMN,
            SchemaChangeKind.CHANGE_TYPE,
        ],
        id="all_changes",
    ),
]

SCHEMA_CHANGE_PARAMS = SINGLE_CHANGES + MULTI_CHANGES + ALL_CHANGES

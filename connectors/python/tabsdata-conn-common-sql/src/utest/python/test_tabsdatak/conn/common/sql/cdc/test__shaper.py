#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal

import pyarrow as pa
import pytest
import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc._encoder import row_to_record
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc._shaper import CdcRowShaper
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_INITIAL_LOAD_TX,
    OP_INSERT,
    OP_LOAD,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

TABLE = "s.t"
TS = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

# One column per Arrow type the CDC schema builder can produce.
ZOO = (
    ("c_int", sa_types.Integer(), [1, None]),
    ("c_big", sa_types.BigInteger(), [2**40, None]),
    ("c_small", sa_types.SmallInteger(), [7, None]),
    ("c_float", sa_types.Float(precision=53), [1.5, None]),
    ("c_num", sa_types.Numeric(precision=10, scale=2), [Decimal("1.50"), None]),
    ("c_bool", sa_types.Boolean(), [True, None]),
    ("c_str", sa_types.String(length=10), ["x", None]),
    ("c_text", sa_types.Text(), ["long", None]),
    ("c_date", sa_types.Date(), [date(2026, 1, 1), None]),
    ("c_time", sa_types.Time(), [time(1, 2, 3), None]),
    ("c_dt", sa_types.DateTime(), [datetime(2026, 1, 1, 1, 2, 3), None]),
    ("c_dttz", sa_types.DateTime(timezone=True), [TS, None]),
    ("c_bin", sa_types.LargeBinary(), [b"\x00\x01", None]),
)

FORMATS = [
    CdcFormat(columns_format="columns", flatten_columns=True),
    CdcFormat(columns_format="columns", flatten_columns=False),
    CdcFormat(columns_format="struct", flatten_columns=False),
    CdcFormat(columns_format="map", flatten_columns=False),
]
FORMAT_IDS = ["flat", "columns", "struct", "map"]


def _schema_version(cdc_format: CdcFormat):
    registry = SchemaRegistry(
        type_mapper=lambda t: t,
        large_column_classifier=lambda t: None,
        cdc_format=cdc_format,
    )
    raw = tuple((name, repr(satype), satype) for name, satype, _ in ZOO)
    return registry.register_from_log(TABLE, raw)


def _rows() -> list[dict]:
    return [
        {name: values[row] for name, _, values in ZOO}
        for row in range(len(ZOO[0][2]))
    ]


def _row_path(schema, cdc_format, rows, *, operation, ordinals) -> pa.Table:
    changes = [
        CdcDataChange(
            operation=operation,
            transaction=CDC_INITIAL_LOAD_TX,
            sequence=ordinal,
            timestamp=TS,
            table=TABLE,
            schema=schema,
            new_values=values,
            old_values={column.name: None for column in schema.columns},
            segment=0,
            ordinal=ordinal,
        )
        for values, ordinal in zip(rows, ordinals)
    ]
    return pa.Table.from_pylist(
        [row_to_record(change, cdc_format) for change in changes],
        schema=schema.pa_schema,
    )


def _shaped(schema, cdc_format, rows, *, operation, ordinals) -> pa.Table:
    columns = {
        column.name: pa.array(
            [values[column.name] for values in rows], type=column.patype
        )
        for column in schema.columns
    } if rows else {}
    return CdcRowShaper(schema, cdc_format).table(
        operation=operation,
        transaction=CDC_INITIAL_LOAD_TX,
        sequences=list(ordinals),
        timestamp=TS,
        segment=0,
        ordinals=list(ordinals),
        columns=columns,
    )


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_shaped_snapshot_rows_equal_the_per_change_path(cdc_format):
    schema = _schema_version(cdc_format)
    rows = _rows()
    ordinals = list(range(1, len(rows) + 1))
    expected = _row_path(
        schema, cdc_format, rows, operation=OP_INSERT.value, ordinals=ordinals
    )
    actual = _shaped(
        schema, cdc_format, rows, operation=OP_INSERT.value, ordinals=ordinals
    )
    assert actual.schema.equals(expected.schema)
    assert actual.equals(expected)


@pytest.mark.parametrize("cdc_format", FORMATS, ids=FORMAT_IDS)
def test_shaped_control_row_equals_the_per_change_path(cdc_format):
    schema = _schema_version(cdc_format)
    empty = [{column.name: None for column in schema.columns}]
    expected = _row_path(
        schema, cdc_format, empty, operation=OP_LOAD.value, ordinals=[0]
    )
    actual = _shaped(schema, cdc_format, [], operation=OP_LOAD.value, ordinals=[0])
    assert actual.equals(expected)


def test_shaped_table_matches_the_declared_schema():
    cdc_format = FORMATS[0]
    schema = _schema_version(cdc_format)
    rows = _rows()
    table = _shaped(
        schema,
        cdc_format,
        rows,
        operation=OP_INSERT.value,
        ordinals=[1, 2],
    )
    assert table.schema.equals(schema.pa_schema)
    assert table.num_rows == 2


def test_missing_column_is_emitted_as_null():
    cdc_format = FORMATS[0]
    schema = _schema_version(cdc_format)
    table = CdcRowShaper(schema, cdc_format).table(
        operation=OP_INSERT.value,
        transaction=CDC_INITIAL_LOAD_TX,
        sequences=[1],
        timestamp=TS,
        segment=0,
        ordinals=[1],
        columns={"c_int": pa.array([5], type=pa.int32())},
    )
    assert table.column("c_int").to_pylist() == [5]
    assert table.column("c_str").to_pylist() == [None]

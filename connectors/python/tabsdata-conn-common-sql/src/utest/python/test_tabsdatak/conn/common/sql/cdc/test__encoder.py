#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
import sqlalchemy.types as sa_types
from sqlalchemy.dialects import mysql

from tabsdatak.conn.common.sql.cdc._encoder import CdcEncoder, converters_of
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, CdcSchemaChange
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.constants import CDC_DATA_COL_OLD_PREFIX, DOT
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode, SqlCdcException
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat
from test_tabsdatak.conn.common.sql.cdc.support import row_to_record

TABLE = "s.t"
FORMAT = CdcFormat()
REGISTRY = SchemaRegistry(
    type_mapper=lambda t: t,
    large_column_classifier=lambda t: None,
    cdc_format=FORMAT,
)
NARROW = REGISTRY.register_from_log(TABLE, (("id", "INTEGER", sa_types.Integer()),))
WIDE = REGISTRY.register_from_log(
    TABLE,
    (
        ("id", "INTEGER", sa_types.Integer()),
        ("name", "VARCHAR", sa_types.String()),
    ),
)


class Offset:
    pass


def change(identifier: int, *, schema=NARROW, table=TABLE, offset=None):
    return CdcDataChange(
        operation="i",
        transaction="tx",
        sequence=identifier,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        table=table,
        schema=schema,
        new_values={"id": identifier, "name": f"n{identifier}"},
        old_values={},
        offset=offset,
        ordinal=identifier,
    )


def encoder() -> CdcEncoder:
    return CdcEncoder(FORMAT, {TABLE: None})


def test_a_tracked_table_change_goes_to_its_table_slot():
    assert encoder().slot(change(1)) == TABLE


def test_an_untracked_table_change_has_no_slot():
    assert encoder().slot(change(1, table="other.table")) is None


def test_an_unsupported_change_type_is_rejected():
    with pytest.raises(NotImplementedError):
        encoder().slot(CdcSchemaChange(table=TABLE))


def test_the_offset_of_a_change_is_its_own():
    offset = Offset()
    assert encoder().offset(change(1, offset=offset)) is offset


def test_a_change_without_an_offset_raises_cdc_11():
    with pytest.raises(SqlCdcException) as raised:
        encoder().offset(change(1, offset=None))
    assert raised.value.api_error is SqlCdcErrorCode.CDC_11


def test_the_size_of_a_change_is_its_estimated_memory():
    record = change(1)
    assert encoder().size(record) == sys.getsizeof(record)


TEXTUAL = REGISTRY.register_from_log(
    TABLE,
    (
        ("id", "INTEGER", sa_types.Integer()),
        ("doc", "JSON", sa_types.JSON()),
        ("uid", "UUID", sa_types.Uuid()),
        ("amount", "NUMERIC", sa_types.Numeric()),
        ("span", "INTERVAL", sa_types.Interval()),
        ("tags", "SET", mysql.SET("a", "b")),
        ("year", "YEAR", mysql.YEAR()),
    ),
)


def test_values_a_text_column_cannot_take_as_they_come_are_made_text():
    """What the drivers hand over for the types that fall back to a text
    column -- a dict, a UUID, a Decimal, a timedelta, a set, an int -- used
    to fail the Arrow conversion; each lands as its text."""
    change = CdcDataChange(
        operation="u",
        transaction="tx",
        sequence=1,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        table=TABLE,
        schema=TEXTUAL,
        new_values={
            "id": 1,
            "doc": {"k": [1, 2]},
            "uid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
            "amount": Decimal("1.50"),
            "span": timedelta(hours=2),
            "tags": {"b", "a"},
            "year": 2026,
        },
        old_values={
            "id": 1,
            "doc": ["x"],
            "uid": None,
            "amount": None,
            "span": None,
            "tags": None,
            "year": None,
        },
        offset=Offset(),
    )

    [table] = list(encoder().encode(TABLE, [change]))

    row = table.to_pylist()[0]
    assert (
        row["doc"],
        row["uid"],
        row["amount"],
        row["span"],
        row["tags"],
        row["year"],
    ) == (
        '{"k":[1,2]}',
        "12345678-1234-5678-1234-567812345678",
        "1.50",
        "2:00:00",
        "a,b",
        "2026",
    )
    old = f"{CDC_DATA_COL_OLD_PREFIX}{DOT}"
    assert (row["id"], row[f"{old}doc"], row[f"{old}uid"]) == (1, '["x"]', None)
    assert change.new_values["doc"] == {"k": [1, 2]}


@pytest.mark.parametrize("columns_format", ["columns", "struct", "map"])
@pytest.mark.parametrize("flatten_columns", [True, False])
def test_every_layout_encodes_as_the_row_at_a_time_reference_does(
    columns_format, flatten_columns
):
    """The columns are assembled a column at a time; the reference builds
    one record per row. Both must agree on every layout, with converted
    values, null images and a schema change in the section's run."""
    cdc_format = CdcFormat(
        columns_format=columns_format, flatten_columns=flatten_columns
    )
    registry = SchemaRegistry(
        type_mapper=lambda t: t,
        large_column_classifier=lambda t: None,
        cdc_format=cdc_format,
    )
    schema = registry.register_from_log(
        TABLE,
        (
            ("id", "INTEGER", sa_types.Integer()),
            ("amount", "NUMERIC", sa_types.Numeric()),
            ("name", "VARCHAR", sa_types.String()),
        ),
    )
    records = [
        CdcDataChange(
            operation=operation,
            transaction="tx",
            sequence=sequence,
            timestamp=datetime(2026, 1, 1, sequence, tzinfo=timezone.utc),
            table=TABLE,
            schema=schema,
            new_values=new_values,
            old_values=old_values,
            offset=Offset(),
            segment=7,
            ordinal=sequence,
        )
        for sequence, operation, new_values, old_values in (
            (1, "i", {"id": 1, "amount": Decimal("1.5"), "name": "a"}, {}),
            (2, "u", {"id": 1, "amount": None, "name": "b"}, {"id": 1, "name": "a"}),
            (3, "d", {"id": 1, "amount": None, "name": None}, {"id": None}),
        )
    ]

    [table] = list(CdcEncoder(cdc_format, {TABLE: None}).encode(TABLE, records))

    assert table.schema == schema.pa_schema
    converters = converters_of(schema)
    assert table.to_pylist() == [
        row_to_record(r, cdc_format, converters) for r in records
    ]


def test_changes_of_one_schema_encode_into_one_table():
    records = [change(1), change(2), change(3)]
    (table,) = encoder().encode(TABLE, records)
    assert table.schema == NARROW.pa_schema
    assert table.to_pylist() == [row_to_record(r, FORMAT) for r in records]


def test_each_run_of_one_schema_encodes_into_its_own_table():
    records = [
        change(1),
        change(2, schema=WIDE),
        change(3, schema=WIDE),
        change(4),
    ]
    tables = list(encoder().encode(TABLE, records))
    assert [table.schema for table in tables] == [
        NARROW.pa_schema,
        WIDE.pa_schema,
        NARROW.pa_schema,
    ]
    assert [table.num_rows for table in tables] == [1, 2, 1]

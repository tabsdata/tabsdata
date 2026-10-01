#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from datetime import datetime, timezone

import pytest
import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc._encoder import CdcEncoder, row_to_record
from tabsdatak.conn.common.sql.cdc._model import CdcDataChange, CdcSchemaChange
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode, SqlCdcException
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

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

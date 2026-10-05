#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the dialect-agnostic initial loader
(:mod:`tabsdatak.conn.common.sql.cdc._initial_load`), covering the
ordering the :class:`InitialLoadSession` contract requires: the resume
position is read once every table has been yielded, never before.

A session whose position is fixed only while its tables are yielded -- on
the first yield or after the last one, as the Db2 one does -- is the case
that catches a loader reading it too early."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlalchemy.types as sa_types

from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.error import TabsdataException

from tabsdatak.conn.common.sql.cdc._initial_load import InitialLoader
from tabsdatak.conn.common.sql.cdc._model import CdcOffset, InitialLoadSession
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat


class _Offset(CdcOffset):
    def __init__(self, value: str):
        self.value = value

    def update(self, other: CdcOffset) -> None:
        self.value = other.value

    def write(self) -> str:
        return self.value

    @classmethod
    def read(cls, data: str) -> CdcOffset:
        return cls(data)


PINNED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


class _PinOnYieldSession(InitialLoadSession):
    """Fixes its position when it pins, and pins on the first yield."""

    def __init__(self, tables: dict):
        super().__init__()
        self._offset: _Offset | None = None
        self._tables = tables
        self.pinned = False

    @property
    def engine(self):
        return MagicMock()

    @property
    def offset(self) -> CdcOffset:
        if self._offset is None:
            raise AssertionError("The position was read before the tables were pinned")
        return self._offset

    def tables(self, tracked_tables):
        self.pinned = True
        self._offset = _Offset("pinned")
        self._timestamp = PINNED_AT
        for tracked_table in tracked_tables.values():
            yield tracked_table, MagicMock()

    def close(self) -> None:
        pass


def _loader(ingester) -> InitialLoader:
    return InitialLoader(
        ingester=ingester,
        chunk_sentinel=MagicMock(),
        cdc_format=MagicMock(),
        large_column_policy=MagicMock(),
        fetch_size=10,
        chunk_size=10,
    )


def _ingester(tracked: dict) -> MagicMock:
    ingester = MagicMock()
    ingester.tracked_tables = tracked
    return ingester


def test_the_position_is_read_after_the_tables_are_pinned(monkeypatch):
    tracked = {"s.t": MagicMock()}
    session = _PinOnYieldSession(tracked)
    loader = _loader(_ingester(tracked))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_the_position_is_read_after_a_session_that_pins_after_the_last_table(
    monkeypatch,
):
    class _PinsAfterTheLastTable(_PinOnYieldSession):
        def tables(self, tracked_tables):
            self._timestamp = PINNED_AT
            for tracked_table in tracked_tables.values():
                yield tracked_table, MagicMock()
            self.pinned = True
            self._offset = _Offset("pinned")

    tracked = {"s.t": MagicMock(), "s.u": MagicMock()}
    session = _PinsAfterTheLastTable(tracked)
    loader = _loader(_ingester(tracked))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_the_position_is_read_even_when_no_table_is_tracked(monkeypatch):
    session = _PinOnYieldSession({})
    loader = _loader(_ingester({}))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    offset = loader.load(session)

    assert session.pinned is True
    assert offset.write() == "pinned"


def test_a_session_that_never_pins_surfaces_its_own_error(monkeypatch):
    class _NeverPins(_PinOnYieldSession):
        def tables(self, tracked_tables):
            yield from ()

    session = _NeverPins({})
    loader = _loader(_ingester({}))
    monkeypatch.setattr(loader, "_load_table", lambda *a, **k: None)

    with pytest.raises(AssertionError, match="before the tables were pinned"):
        loader.load(session)


def test_each_table_loaded_logs_its_rows_as_one_poll_of_the_source(monkeypatch, caplog):
    from tabsdatak.conn.common.sql.cdc import _initial_load

    monkeypatch.setattr(_initial_load, "CdcRowShaper", MagicMock())
    loader = _loader(_ingester({"s.t": MagicMock()}))
    chunks = [MagicMock(num_rows=3), MagicMock(num_rows=4)]
    monkeypatch.setattr(loader, "_read_chunks", lambda *_args: iter(chunks))

    with caplog.at_level("DEBUG", logger="tabsdatak.conn.common._stream._poll"):
        loader._load_table(MagicMock(qualified="s.t"), MagicMock(), PINNED_AT)

    polled = [r.getMessage() for r in caplog.records if "Polled" in r.getMessage()]
    assert [line.split(" from")[0] for line in polled] == ["Polled 7 record(s)"]


def test_every_loaded_row_carries_the_time_the_snapshot_began(monkeypatch):
    tracked = {"s.t": MagicMock(), "s.u": MagicMock()}
    session = _PinOnYieldSession(tracked)
    loader = _loader(_ingester(tracked))
    stamped = []
    monkeypatch.setattr(
        loader,
        "_load_table",
        lambda _table, _connection, timestamp: stamped.append(timestamp),
    )

    loader.load(session)

    assert stamped == [PINNED_AT, PINNED_AT]


def test_the_time_is_not_readable_before_the_snapshot_begins():
    with pytest.raises(TabsdataException) as raised:
        _ = _PinOnYieldSession({}).timestamp
    assert raised.value.api_error is SqlCdcErrorCode.CDC_30


def test_a_loaded_column_falling_back_to_text_gets_its_values_as_text():
    registry = SchemaRegistry(
        type_mapper=lambda t: t,
        large_column_classifier=lambda t: None,
        cdc_format=CdcFormat(),
    )
    schema = registry.register_from_log(
        "s.t",
        (
            ("id", "INTEGER", sa_types.Integer()),
            ("doc", "JSON", sa_types.JSON()),
            ("uid", "UUID", sa_types.Uuid()),
            ("amount", "NUMERIC", sa_types.Numeric()),
        ),
    )
    arrow_schema = pa.schema([pa.field(c.name, c.patype) for c in schema.columns])
    loader = _loader(_ingester({"s.t": MagicMock()}))
    rows = [
        (
            1,
            {"k": 1},
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            Decimal("2.5"),
        ),
        (2, None, None, None),
    ]

    table = loader._to_arrow(rows, schema, arrow_schema, MagicMock(qualified="s.t"))

    assert table.to_pylist() == [
        {
            "id": 1,
            "doc": '{"k":1}',
            "uid": "12345678-1234-5678-1234-567812345678",
            "amount": "2.5",
        },
        {"id": 2, "doc": None, "uid": None, "amount": None},
    ]

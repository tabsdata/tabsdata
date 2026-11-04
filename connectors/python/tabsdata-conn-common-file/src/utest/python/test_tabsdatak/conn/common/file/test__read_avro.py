#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal

import fastavro
import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._avro import Avro
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]

INSTANT = datetime(2024, 1, 2, 3, 4, 5, 678000, tzinfo=timezone.utc)
NANOS = 1_700_000_000_123_456_789
ID = uuid.UUID(int=1)


def avro(fields: list, records: list[dict], codec: str = "deflate") -> bytes:
    schema = {
        "type": "record",
        "name": "r",
        "fields": [{"name": name, "type": kind} for name, kind in fields],
    }
    buffer = io.BytesIO()
    fastavro.writer(buffer, schema, records, codec=codec)
    return buffer.getvalue()


def read(content: bytes, *, batch_rows: int = 1000):
    return Avro(batch_rows=batch_rows).read(Source(content))


@pytest.mark.parametrize(
    "kind, value, dtype, served",
    [
        ("int", 1, pl.Int32, 1),
        (["null", "long"], None, pl.Int64, None),
        ("float", 1.5, pl.Float32, 1.5),
        ("double", 2.5, pl.Float64, 2.5),
        ("boolean", True, pl.Boolean, True),
        ("string", "x", pl.String, "x"),
        ("bytes", b"\x00\x01", pl.Binary, b"\x00\x01"),
        ({"type": "int", "logicalType": "date"}, date(2024, 1, 2), pl.Date, None),
        (
            {"type": "int", "logicalType": "time-millis"},
            time(1, 2, 3, 4000),
            pl.Time,
            None,
        ),
        (
            {"type": "long", "logicalType": "time-micros"},
            time(1, 2, 3, 4),
            pl.Time,
            None,
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            INSTANT,
            pl.Datetime("ms", "UTC"),
            None,
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            INSTANT,
            pl.Datetime("us", "UTC"),
            None,
        ),
        (
            {"type": "long", "logicalType": "timestamp-nanos"},
            NANOS,
            pl.Datetime("ns", "UTC"),
            datetime(2023, 11, 14, 22, 13, 20, 123456, tzinfo=timezone.utc),
        ),
        (
            {"type": "long", "logicalType": "local-timestamp-micros"},
            INSTANT.replace(tzinfo=None),
            pl.Datetime("us"),
            None,
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2},
            Decimal("12.34"),
            pl.Decimal(10, 2),
            None,
        ),
        (
            {"type": "string", "logicalType": "uuid"},
            str(ID),
            pl.Binary,
            ID.bytes,
        ),
        (
            {"type": "enum", "name": "E", "symbols": ["A", "B"]},
            "B",
            pl.Categorical,
            None,
        ),
        ({"type": "array", "items": "long"}, [1, 2], pl.List(pl.Int64), None),
        (
            {"type": "map", "values": "long"},
            {"k": 3},
            pl.Map(pl.String, pl.Int64),
            None,
        ),
        (
            {"type": "record", "name": "n", "fields": [{"name": "x", "type": "long"}]},
            {"x": 4},
            pl.Struct({"x": pl.Int64}),
            None,
        ),
        ({"type": "fixed", "name": "F", "size": 4}, b"abcd", pl.Binary, None),
    ],
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_types_land_as_the_transporter_landed_them(
    tmp_path, kind, value, dtype, served
):
    landed = land(read(avro([("v", kind)], [{"v": value}])), tmp_path)
    assert landed.served.schema["v"] == dtype
    assert landed.served.to_dicts()[0]["v"] == (value if served is None else served)


@pytest.mark.parametrize(
    "kind, reason",
    [
        (["null", "string", "long"], "'v' is a union of 2 types"),
        (
            {"type": "fixed", "name": "D", "size": 12, "logicalType": "duration"},
            "'v' is an Avro duration",
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 40, "scale": 2},
            "'v' is a decimal of precision 40",
        ),
    ],
    ids=["union", "duration", "wide decimal"],
)
def test_types_no_column_holds_are_refused_naming_the_field(kind, reason):
    with pytest.raises(ValueError, match=reason):
        read(avro([("v", kind)], []))


def test_uuids_nested_in_arrays_maps_and_records_are_bytes(tmp_path):
    identifier = {"type": "string", "logicalType": "uuid"}
    fields = [
        ("ids", {"type": "array", "items": ["null", identifier]}),
        ("by", {"type": "map", "values": identifier}),
        (
            "in",
            {
                "type": "record",
                "name": "n",
                "fields": [{"name": "id", "type": identifier}],
            },
        ),
    ]
    record = {"ids": [str(ID), None], "by": {"k": str(ID)}, "in": {"id": str(ID)}}
    served = land(read(avro(fields, [record])), tmp_path).served
    assert served.row(0) == ([ID.bytes, None], {"k": ID.bytes}, {"id": ID.bytes})


def test_a_named_type_used_twice_reads_both_times(tmp_path):
    point = {"type": "record", "name": "P", "fields": [{"name": "x", "type": "long"}]}
    content = avro([("a", point), ("b", "P")], [{"a": {"x": 1}, "b": {"x": 2}}])
    served = land(read(content), tmp_path).served
    assert served.to_dicts() == [{"a": {"x": 1}, "b": {"x": 2}}]


@pytest.mark.parametrize(
    "codec", ["null", "deflate", "snappy", "zstandard", "bzip2", "xz"]
)
def test_every_avro_block_codec_reads_in_batches(tmp_path, codec):
    records = [{"id": n, "name": f"n{n}"} for n in range(7)]
    content = avro([("id", "long"), ("name", "string")], records, codec=codec)
    served = land(read(content, batch_rows=2), tmp_path).served
    assert served.to_dicts() == records


def test_a_block_codec_outside_the_avro_specification_is_refused(tmp_path):
    content = avro([("id", "long")], [{"id": 1}], codec="lz4")
    with pytest.raises(pl.exceptions.ComputeError, match="compression codec 'lz4'"):
        land(read(content), tmp_path)


def test_a_file_without_records_lands_its_columns(tmp_path):
    served = land(read(avro([("id", "long")], [])), tmp_path).served
    assert served.height == 0
    assert served.schema == pl.Schema({"id": pl.Int64})


def test_a_file_polars_wrote_reads_back(tmp_path):
    frame = pl.DataFrame({"id": [1, 2], "name": ["alice", "bob"]})
    buffer = io.BytesIO()
    frame.write_avro(buffer)
    assert land(read(buffer.getvalue()), tmp_path).served.equals(frame)


def test_the_file_is_read_once_and_closed(tmp_path):
    source = Source(avro([("id", "long")], [{"id": 1}]))
    land(Avro().read(source), tmp_path)
    assert len(source.streams) == 1
    assert source.streams[0].closed


def test_a_file_that_is_not_avro_is_refused_and_closed():
    source = Source(b"not avro")
    with pytest.raises(ValueError):
        Avro().read(source)
    assert source.streams[0].closed

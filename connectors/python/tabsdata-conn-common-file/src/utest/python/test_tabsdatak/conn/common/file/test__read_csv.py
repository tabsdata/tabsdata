#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._csv import AutoCsv
from tabsdatak.spi import CsvParseOptions, SourceFormat
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]

BLOCK = 8

ORDERS = (
    b"id,amount,note,day,paid\r\n"
    b'1,1.5,"multi\nline",2024-01-01,true\n'
    b'2,,"a, b",2024-01-02,False\n'
    b"3,7,plain,2024-01-03,true\n"
    b'4,8,"q""uote",,\n'
)


def read(content: bytes, options: dict | None = None, *, block_size: int = BLOCK):
    return AutoCsv(options, block_size=block_size).read(Source(content))


@pytest.mark.parametrize(
    "block_size", [1, BLOCK, 1 << 20], ids=["byte", "small", "one"]
)
def test_blocks_land_the_strings_polars_reads_from_the_whole_file(tmp_path, block_size):
    landed = land(read(ORDERS, block_size=block_size), tmp_path)
    assert landed.data.equals(pl.read_csv(ORDERS, infer_schema=False))


def test_the_types_are_inferred_over_the_whole_file(tmp_path):
    landed = land(read(ORDERS), tmp_path)
    expected = pl.read_csv(ORDERS, infer_schema_length=None)
    assert landed.schema == expected.schema
    assert landed.served.equals(expected)


@pytest.mark.parametrize(
    "late, dtype",
    [("x", pl.String), ("2.5", pl.Float64), ("99999999999999999999", pl.Int128)],
    ids=["text", "float", "wider integer"],
)
def test_a_value_in_the_last_block_widens_the_type(tmp_path, late, dtype):
    content = ("n\n" + "1\n" * 50 + f"{late}\n").encode()
    landed = land(read(content), tmp_path)
    assert landed.schema == pl.Schema({"n": dtype})
    assert landed.served["n"].dtype == dtype


def test_a_block_where_a_column_is_empty_does_not_type_it(tmp_path):
    content = b"a,b\n" + b"1,\n" * 20 + b"2,7\n"
    landed = land(read(content), tmp_path)
    assert landed.schema == pl.Schema({"a": pl.Int64, "b": pl.Int64})


def test_a_column_never_holding_a_value_is_a_string(tmp_path):
    landed = land(read(b"a,b\n1,\n2,\n"), tmp_path)
    assert landed.schema == pl.Schema({"a": pl.Int64, "b": pl.String})


def test_a_header_without_rows_lands_no_rows_of_strings(tmp_path):
    landed = land(read(b"a,b\n"), tmp_path)
    assert landed.data.height == 0
    assert landed.schema == pl.Schema({"a": pl.String, "b": pl.String})


def test_an_empty_file_is_refused_as_polars_refuses_it(tmp_path):
    with pytest.raises(pl.exceptions.NoDataError):
        read(b"")


def test_an_empty_file_lands_nothing_when_allowed(tmp_path):
    landed = land(read(b"", {"raise_if_empty": False}), tmp_path)
    assert landed.data.shape == (0, 0)


def test_dates_are_left_as_strings_unless_asked_for(tmp_path):
    landed = land(read(ORDERS), tmp_path)
    assert landed.schema["day"] == pl.String


def test_dates_are_detected_when_asked_for(tmp_path):
    landed = land(read(ORDERS, {"try_parse_dates": True}), tmp_path)
    expected = pl.read_csv(ORDERS, infer_schema_length=None, try_parse_dates=True)
    assert landed.schema["day"] == pl.Date
    assert landed.served.equals(expected)


def test_decimal_comma_floats_are_served_as_floats(tmp_path):
    content = b"price;name\n1,5;a\n2,25;b\n"
    landed = land(read(content, {"separator": ";", "decimal_comma": True}), tmp_path)
    assert landed.metadata.format is SourceFormat.CSV
    assert landed.metadata.parsing == CsvParseOptions(decimal_comma=True)
    assert not landed.metadata.typed
    assert landed.data["price"].to_list() == ["1,5", "2,25"]
    assert landed.served["price"].to_list() == [1.5, 2.25]


@pytest.mark.parametrize(
    "content, options",
    [
        (b"a;b\n1;x\n2;y\n", {"separator": ";"}),
        (b"noise\nnoise\na,b\n1,x\n2,y\n", {"skip_rows": 2}),
        (b"a,b\nunit,unit\n1,x\n2,y\n", {"skip_rows_after_header": 1}),
        (b"# note\na,b\n1,x\n# note\n2,y\n", {"comment_prefix": "#"}),
        (b"a,b\n1,NA\nNA,y\n", {"null_values": ["NA"]}),
        (b"1,x\n2,y\n3,z\n", {"has_header": False}),
        (b"a,b|1,x|2,y|", {"eol_char": "|"}),
        (b"a,b\n1,'x\ny'\n2,'z'\n", {"quote_char": "'"}),
        (b'a,b\n1,"x\n2,y\n', {"quote_char": None}),
    ],
    ids=[
        "separator",
        "skip rows",
        "skip rows after header",
        "comment prefix",
        "null values",
        "no header",
        "eol char",
        "quote char",
        "no quote char",
    ],
)
def test_the_source_options_mean_what_they_mean_to_polars(tmp_path, content, options):
    landed = land(read(content, options, block_size=3), tmp_path)
    assert landed.data.equals(pl.read_csv(content, infer_schema=False, **options))
    expected = pl.read_csv(content, infer_schema_length=None, **options)
    assert landed.served.equals(expected)


def test_the_file_is_read_once_and_closed(tmp_path):
    source = Source(ORDERS)
    land(AutoCsv(block_size=BLOCK).read(source), tmp_path)
    assert len(source.streams) == 1
    assert source.streams[0].closed

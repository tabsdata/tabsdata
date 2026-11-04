#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Callable

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._csv import AutoCsv

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._ndjson import AutoNdjson
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]

BLOCK_SIZES = pytest.mark.parametrize(
    "block_size", [1, 5, 1 << 20], ids=["byte", "small", "one"]
)

CSV_CASES = {
    "short rows": (b"a,b,c\n1,2,3\n4,5\n6\n", {}),
    "short rows after full ones": (b"a,b,c\n" + b"1,2,3\n" * 20 + b"4\n5,6\n", {}),
    "wide rows truncated": (
        b"a,b\n1,2\n" * 10 + b"3,4,5,6\n",
        {"truncate_ragged_lines": True},
    ),
    "quoted separators": (b'a,b\n"1,5",x\n"2,5","y,z"\n', {}),
    "quoted newlines": (b'a,b\n1,"x\ny"\n2,"\n\n"\n3,z\n', {}),
    "quoted quotes": (b'a,b\n1,"say ""hi"""\n2,""""\n', {}),
    "crlf": (b"a,b\r\n1,x\r\n2,y\r\n", {}),
    "bom": (b"\xef\xbb\xbfa,b\n1,x\n2,y\n", {}),
    "duplicate header names": (b"a,a,b\n1,2,3\n4,5,6\n", {}),
    "empty header names": (b"a,,\n1,2,3\n4,5,6\n", {}),
    "comment lines": (b"# head\na,b\n1,x\n# mid\n2,y\n", {"comment_prefix": "#"}),
    "comment prefix like a header letter": (
        b"a0\nx,y\n1,2\n3,4\n",
        {"comment_prefix": "a"},
    ),
    "separator like a header letter": (b"xay\n1a2\n3a4\n", {"separator": "a"}),
    "null values": (b"a,b\n1,NA\nNA,y\n", {"null_values": ["NA"]}),
    "no header": (b"1,x\n2,y\n3\n", {"has_header": False}),
    "header only": (b"a,b\n", {}),
}


def read_csv(content: bytes, options: dict, block_size: int):
    return AutoCsv(options, block_size=block_size).read(Source(content))


@BLOCK_SIZES
@pytest.mark.parametrize("case", list(CSV_CASES))
def test_csv_rows_land_and_serve_as_polars_reads_the_whole_file(
    tmp_path, case, block_size
):
    content, options = CSV_CASES[case]
    landed = land(read_csv(content, options, block_size), tmp_path)
    assert landed.data.equals(pl.read_csv(content, infer_schema=False, **options))
    expected = pl.read_csv(content, infer_schema_length=None, **options)
    assert landed.served.equals(expected)


@BLOCK_SIZES
@pytest.mark.parametrize(
    "content",
    [b"a,b\n1,2\n3,4,5\n", b"a,b\n" + b"1,2\n" * 20 + b"3,4,5,6\n"],
    ids=["early", "late"],
)
def test_a_csv_row_wider_than_the_header_fails_the_run(tmp_path, content, block_size):
    # Pins polars' behaviour until the schema widens to fit it (PW3, D-b).
    with pytest.raises(pl.exceptions.ComputeError, match="found more fields"):
        land(read_csv(content, {}, block_size), tmp_path)


def read_ndjson(content: bytes, block_size: int):
    return AutoNdjson(block_size=block_size).read(Source(content))


def test_ndjson_null_and_absent_fields_both_serve_null(tmp_path):
    content = b'{"a": null, "b": 1}\n{"b": 2}\n{"a": 3, "b": null}\n'
    landed = land(read_ndjson(content, 5), tmp_path)
    assert landed.served.to_dict(as_series=False) == {
        "a": [None, None, 3],
        "b": [1, 2, None],
    }


def test_an_ndjson_line_longer_than_a_block_lands_whole(tmp_path):
    long = "x" * 1000
    content = f'{{"a": 1, "s": "{long}"}}\n{{"a": 2}}\n'.encode()
    landed = land(read_ndjson(content, 16), tmp_path)
    assert landed.served.to_dict(as_series=False) == {
        "a": [1, 2],
        "s": [long, None],
    }


def test_an_ndjson_file_starting_with_a_bom_is_refused_as_polars_refuses_it(
    tmp_path,
):
    content = b'\xef\xbb\xbf{"a": 1}\n{"a": 2}\n'
    with pytest.raises(pl.exceptions.ComputeError):
        pl.read_ndjson(content)
    with pytest.raises(pl.exceptions.ComputeError, match="Line 1 is not a JSON"):
        land(read_ndjson(content, 5), tmp_path)


ORDERS = (
    b"id,customer,amount,note\n"
    b'1,c1,10.5,"first\nline"\n'
    b"2,c2,3\n"
    b'3,c1,,"a, b"\n'
    b"4,c3,7.25,plain\n"
)
MORE_ORDERS = b"id,customer,amount,note\n5,c2,1\n6,c1,2.5,x\n"
CUSTOMERS = (
    b'{"customer": "c1", "name": "Ann", "tags": ["a"]}\n'
    b'{"customer": "c2", "tier": 2}\n'
    b"\n"
    b'{"name": "Cy", "customer": "c3", "tier": null}\n'
)

PIPELINES: dict[
    str, Callable[[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame], pl.LazyFrame]
] = {
    "select": lambda orders, more, customers: orders.select(
        "id", (pl.col("amount") * 2).alias("double")
    ),
    "filter": lambda orders, more, customers: orders.filter(
        pl.col("amount").is_null() | (pl.col("amount") > 5)
    ),
    "join": lambda orders, more, customers: orders.join(
        customers, on="customer", how="left"
    ).sort("id"),
    "group by": lambda orders, more, customers: (
        orders
        .group_by("customer")
        .agg(pl.col("amount").sum(), pl.len())
        .sort("customer")
    ),
    "concat": lambda orders, more, customers: pl.concat([orders, more]).sort("id"),
}


@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_served_frames_run_pipelines_as_polars_reads_do(tmp_path, pipeline):
    def served(name: str, table) -> pl.LazyFrame:
        folder = tmp_path / name
        folder.mkdir()
        return land(table, folder).served.lazy()

    orders = served("orders", read_csv(ORDERS, {}, 5))
    more = served("more", read_csv(MORE_ORDERS, {}, 5))
    customers = served("customers", read_ndjson(CUSTOMERS, 5))
    run = PIPELINES[pipeline]
    expected = run(
        pl.read_csv(ORDERS, infer_schema_length=None).lazy(),
        pl.read_csv(MORE_ORDERS, infer_schema_length=None).lazy(),
        pl.read_ndjson(CUSTOMERS, infer_schema_length=None).lazy(),
    )
    assert run(orders, more, customers).collect().equals(expected.collect())

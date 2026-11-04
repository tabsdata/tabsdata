#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import polars as pl
import pyarrow as pa
import pytest

from tabsdatak.common._storage.landed import (
    StoredMetadata,
    StoredMetadataError,
    decode_dtype,
    encode_dtype,
    parse,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._handlers.landing import (
    LandedInputs,
    Landing,
    MetadataMismatchError,
    NotLandedError,
)
from tabsdatak.spi import (
    CsvParseOptions,
    FrameMetadata,
    Layout,
    LazyFrameInput,
    SourceFormat,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def landing(root: Path) -> Landing:
    return Landing(Location(uri=(root / "landing").as_uri(), env_prefix="ROOT"))


def local(location: Location) -> Path:
    return Path(url2pathname(urlparse(location.uri).path))


def land(root: Path, frame: pl.LazyFrame, metadata=None, *, position=0, index=0):
    return landing(root).land(position, index, LazyFrameInput(frame, metadata))


def stored(location: Location) -> dict:
    rows = pl.read_parquet(local(location)).iter_rows()
    return {kind: json.loads(content) for kind, content in rows}


def test_data_lands_with_its_own_types_and_its_metadata(tmp_path):
    frame = pl.LazyFrame(
        {
            "id": [1, 2, 3],
            "day": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
        },
    )
    landed = land(tmp_path, frame, FrameMetadata(columns={"@query": "select 1"}))
    assert landed.rows == 3
    assert landed.data.uri.endswith("/landing/0.0.t")
    assert landed.metadata.uri.endswith("/landing/0.0.m")
    assert pl.read_parquet(local(landed.data)).equals(frame.collect())
    assert pl.read_parquet(local(landed.metadata)).schema == pl.Schema(
        {"kind": pl.String, "content": pl.String},
    )
    assert stored(landed.metadata) == {
        "layout": "columnar",
        "typed": True,
        "columns": {"@query": "select 1"},
    }


def test_data_without_metadata_is_served_as_it_landed(tmp_path):
    frame = pl.LazyFrame({"id": [1, 2]})
    landed = land(tmp_path, frame)
    served, metadata = landed.table.scan()
    assert served.collect().equals(frame.collect())
    assert metadata == FrameMetadata()


def test_strings_land_as_they_are_and_are_served_with_the_schema(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("id,paid,day\n00123,true,31-01-2024\n7,False,01-02-2024\n")
    schema = pl.scan_csv(
        source,
        infer_schema_length=None,
        try_parse_dates=True,
    ).collect_schema()
    landed = land(
        tmp_path, pl.scan_csv(source, infer_schema=False), FrameMetadata(schema)
    )
    assert pl.read_parquet(local(landed.data)).to_dict(as_series=False) == {
        "id": ["00123", "7"],
        "paid": ["true", "False"],
        "day": ["31-01-2024", "01-02-2024"],
    }
    assert stored(landed.metadata)["schema"] == {
        "id": "Int64",
        "paid": "Boolean",
        "day": "Date",
    }
    served, metadata = landed.table.scan()
    assert served.collect().equals(pl.read_csv(source, schema=schema))
    assert metadata.schema == schema


def test_columns_already_of_their_type_are_served_as_they_are(tmp_path):
    frame = pl.LazyFrame({"id": ["1"], "amount": [2.5]})
    schema = pl.Schema({"id": pl.Int64, "amount": pl.Float64})
    served, _ = land(tmp_path, frame, FrameMetadata(schema)).table.scan()
    assert served.collect().to_dict(as_series=False) == {"id": [1], "amount": [2.5]}


def test_an_empty_frame_lands_with_no_rows(tmp_path):
    frame = pl.LazyFrame(schema={"id": pl.Int64})
    landed = land(tmp_path, frame)
    assert landed.rows == 0
    assert pl.read_parquet(local(landed.data)).schema == frame.collect_schema()


def test_a_schema_must_name_the_data_columns(tmp_path):
    with pytest.raises(MetadataMismatchError, match=r"names columns \['code'\]"):
        land(
            tmp_path,
            pl.LazyFrame({"id": ["1"]}),
            FrameMetadata(pl.Schema({"code": pl.Int64})),
        )


def test_columns_of_another_type_are_cast_to_theirs(tmp_path):
    frame = pl.LazyFrame({"id": [1], "flag": [1]})
    schema = pl.Schema({"id": pl.Float64, "flag": pl.Boolean})
    served, _ = land(tmp_path, frame, FrameMetadata(schema)).table.scan()
    assert served.collect().to_dict(as_series=False) == {
        "id": [1.0],
        "flag": [True],
    }


def test_a_value_that_cannot_be_cast_fails_when_served(tmp_path):
    frame = pl.LazyFrame({"id": [300]})
    landed = land(tmp_path, frame, FrameMetadata(pl.Schema({"id": pl.Int8})))
    served, _ = landed.table.scan()
    with pytest.raises(pl.exceptions.InvalidOperationError):
        served.collect()


def test_metadata_learnt_while_streaming_is_asked_for_once_the_data_landed(
    tmp_path,
):
    streamed: list[str] = []

    def batches():
        for value in ("1", "2"):
            streamed.append(value)
            yield pa.record_batch({"id": [value]})

    def metadata() -> FrameMetadata:
        assert streamed == ["1", "2"]
        return FrameMetadata(pl.Schema({"id": pl.Int64}))

    reader = pa.RecordBatchReader.from_batches(
        pa.schema({"id": pa.string()}),
        batches(),
    )
    landed = land(tmp_path, pl.scan_arrow_c_stream(reader), metadata)
    assert landed.rows == 2
    assert stored(landed.metadata)["schema"] == {"id": "Int64"}


def test_metadata_learnt_while_streaming_must_match_the_data(tmp_path):
    with pytest.raises(MetadataMismatchError, match=r"names columns \['code'\]"):
        land(
            tmp_path,
            pl.LazyFrame({"id": ["1"]}),
            lambda: FrameMetadata(pl.Schema({"code": pl.Int64})),
        )


def test_ndjson_lands_as_lines_and_is_served_as_their_fields(tmp_path):
    lines = pl.LazyFrame({"line": ['{"id": 1, "name": "a"}', '{"id": 2}']})
    landed = land(
        tmp_path,
        lines,
        FrameMetadata(
            pl.Schema({"id": pl.Int64, "name": pl.String}),
            layout=Layout.SERIALIZED,
            format=SourceFormat.NDJSON,
            typed=False,
        ),
    )
    assert pl.read_parquet(local(landed.data)).equals(lines.collect())
    served, _ = landed.table.scan()
    assert served.collect().to_dict(as_series=False) == {
        "id": [1, 2],
        "name": ["a", None],
    }


@pytest.mark.parametrize(
    "frame, schema, match",
    [
        (
            pl.LazyFrame({"a": ["{}"], "b": ["{}"]}),
            pl.Schema({"a": pl.Int64}),
            "serialized data lands as one String column",
        ),
        (
            pl.LazyFrame({"line": [1]}),
            pl.Schema({"a": pl.Int64}),
            "serialized data lands as one String column",
        ),
        (
            pl.LazyFrame({"line": ["{}"]}),
            None,
            "serialized data is served with a schema",
        ),
    ],
    ids=["two columns", "typed column", "no schema"],
)
def test_serialized_data_is_one_string_column_served_with_a_schema(
    tmp_path, frame, schema, match
):
    with pytest.raises(MetadataMismatchError, match=match):
        land(
            tmp_path,
            frame,
            FrameMetadata(
                schema,
                layout=Layout.SERIALIZED,
                format=SourceFormat.NDJSON,
                typed=False,
            ),
        )


def test_serialized_rows_of_a_format_not_split_when_served_are_refused(tmp_path):
    with pytest.raises(MetadataMismatchError, match="serialized CSV rows"):
        land(
            tmp_path,
            pl.LazyFrame({"line": ["1,a"]}),
            FrameMetadata(
                pl.Schema({"id": pl.Int64, "name": pl.String}),
                layout=Layout.SERIALIZED,
                format=SourceFormat.CSV,
                typed=False,
            ),
        )


TYPES = [
    pl.Null(),
    pl.Boolean(),
    pl.Int8(),
    pl.Int128(),
    pl.UInt64(),
    pl.Float32(),
    pl.Float64(),
    pl.String(),
    pl.Binary(),
    pl.Date(),
    pl.Time(),
    pl.Datetime("us"),
    pl.Datetime("ns", "Europe/Madrid"),
    pl.Duration("ms"),
    pl.Decimal(10, 2),
    pl.Enum(["a", "b"]),
    pl.List(pl.Int64),
    pl.Array(pl.Float64, 3),
    pl.Struct({"x": pl.Int64, "y": pl.List(pl.Struct({"z": pl.String}))}),
]


@pytest.mark.parametrize("dtype", TYPES, ids=[str(dtype) for dtype in TYPES])
def test_a_stored_type_reads_back_as_it_was(dtype):
    encoded = json.loads(json.dumps(encode_dtype(dtype)))
    assert decode_dtype(encoded) == dtype


def test_stored_metadata_reads_back_as_it_was():
    metadata = FrameMetadata(
        pl.Schema({"b": pl.Struct({"y": pl.Int64, "x": pl.Date}), "a": pl.String}),
        format=SourceFormat.CSV,
        typed=False,
        parsing=CsvParseOptions(decimal_comma=True),
        columns={"@file": "a.json", "@size": 7},
    )
    read = StoredMetadata.read(StoredMetadata.of(metadata))
    assert read == metadata
    assert list(read.schema) == ["b", "a"]
    assert list(read.schema["b"].to_schema()) == ["y", "x"]


def test_a_type_that_cannot_be_stored_is_refused_at_landing(tmp_path):
    frame = pl.LazyFrame({"o": ["a"]}, schema={"o": pl.Categorical})
    with pytest.raises(MetadataMismatchError, match="cannot be stored"):
        land(tmp_path, frame, FrameMetadata(frame.collect_schema()))


@pytest.mark.parametrize(
    "kind, content",
    [
        ("layout", '"rows"'),
        ("format", '"xml"'),
        ("typed", '"yes"'),
        ("parsing", '{"decimal_comma": true}'),
        ("units", "{}"),
    ],
    ids=["layout", "format", "typed", "parsing without format", "kind"],
)
def test_unknown_stored_metadata_is_refused(kind, content):
    with pytest.raises(StoredMetadataError):
        StoredMetadata.read(pl.DataFrame({"kind": [kind], "content": [content]}))


# --- serving ------------------------------------------------------------------

COLUMNS = {
    "integers": ["1", "-2", "00123"],
    "large integers": ["99999999999999999999", "1"],
    "floats": ["1.5", "1e3", "inf"],
    "booleans": ["TRUE", "false", "True"],
    "dates": ["2024-01-31", "2024-02-01"],
    "day first dates": ["31-01-2024", "01-02-2024"],
    "dotted dates": ["31.01.2024", "01.02.2024"],
    "datetimes": ["2024-01-31 10:00:00", "2024-02-01 11:30:00.123456"],
    "zoned datetimes": ["2024-01-31T10:00:00+02:00", "2024-02-01T10:00:00Z"],
    "times": ["10:00:00", "11:00:00.5"],
    "short times": ["10:00", "11:30"],
    "texts": ["a", "b"],
    "missing values": ["", "1"],
}


def csv_of(values: list[str]) -> str:
    return "c\n" + "\n".join(values) + "\n"


@pytest.mark.parametrize("values", COLUMNS.values(), ids=COLUMNS.keys())
def test_parse_reads_strings_as_polars_reads_the_csv(values):
    expected = pl.read_csv(
        io.StringIO(csv_of(values)),
        infer_schema_length=None,
        try_parse_dates=True,
    )
    strings = pl.read_csv(io.StringIO(csv_of(values)), infer_schema=False)
    served = strings.select(parse("c", expected.schema["c"]))
    assert served.equals(expected)


def test_a_column_polars_cannot_read_fails_the_same_way():
    mixed = csv_of(["10:00:00", "11:30"])
    with pytest.raises(pl.exceptions.InvalidOperationError):
        pl.read_csv(io.StringIO(mixed), infer_schema_length=None, try_parse_dates=True)
    strings = pl.read_csv(io.StringIO(mixed), infer_schema=False)
    with pytest.raises(pl.exceptions.InvalidOperationError):
        strings.select(parse("c", pl.Time))


@pytest.mark.parametrize(
    "value, dtype",
    [
        ("yes", pl.Boolean),
        ("x", pl.Int64),
        ("2024-13-45", pl.Date),
    ],
)
def test_parse_refuses_a_value_of_another_type(value, dtype):
    with pytest.raises(pl.exceptions.PolarsError):
        pl.DataFrame({"c": [value]}).select(parse("c", dtype))


@pytest.mark.parametrize(
    "values",
    [["1,5", "-2,25"], ["1e3", "0,5"], ["7", "8,0"]],
    ids=["fractions", "exponent", "mixed"],
)
def test_parse_reads_decimal_comma_floats_as_polars_reads_the_csv(values):
    text = csv_of(values)
    options = {"separator": ";", "decimal_comma": True}
    expected = pl.read_csv(io.StringIO(text), infer_schema_length=None, **options)
    assert expected.schema["c"] == pl.Float64
    strings = pl.read_csv(io.StringIO(text), infer_schema=False, **options)
    served = strings.select(parse("c", pl.Float64, decimal_comma=True))
    assert served.equals(expected)


def test_decimal_comma_strings_are_served_as_floats(tmp_path):
    source = tmp_path / "prices.csv"
    source.write_text("price;name\n1,5;a\n2,25;b\n")
    landed = land(
        tmp_path,
        pl.scan_csv(source, infer_schema=False, separator=";").select("price"),
        FrameMetadata(
            pl.Schema({"price": pl.Float64}),
            format=SourceFormat.CSV,
            typed=False,
            parsing=CsvParseOptions(decimal_comma=True),
        ),
    )
    served, metadata = landed.table.scan()
    assert served.collect()["price"].to_list() == [1.5, 2.25]
    assert metadata.parsing == CsvParseOptions(decimal_comma=True)


def test_parse_options_default_to_the_formats(tmp_path):
    landed = land(
        tmp_path,
        pl.LazyFrame({"id": ["1"]}),
        FrameMetadata(
            pl.Schema({"id": pl.Int64}), format=SourceFormat.CSV, typed=False
        ),
    )
    assert stored(landed.metadata)["parsing"] == {"decimal_comma": False}
    _, metadata = landed.table.scan()
    assert metadata.parsing == CsvParseOptions()


def landed_record(
    root: Path, position: int, index: int, values: list[int]
) -> InputRecord:
    landed = land(
        root,
        pl.LazyFrame({"id": values}),
        FrameMetadata(columns={"@part": index}),
        position=position,
        index=index,
    )
    return InputRecord(location=landed.data, metadata_location=landed.metadata)


def test_landed_inputs_get_their_metadata_and_system_columns(tmp_path):
    info = Info.model_validate(info_payload())
    record = landed_record(tmp_path, 0, 0, [1, 2])
    served = LandedInputs(info).scan(record).collect()
    expected = (
        LandedInputs(info).stamp(pl.LazyFrame({"id": [1, 2]}), {"@part": 0}).collect()
    )
    assert served.columns == expected.columns
    assert served.select("id", "@td.@part").equals(expected.select("id", "@td.@part"))


def test_landed_inputs_number_every_file_from_one_sequence(tmp_path):
    inputs = LandedInputs(Info.model_validate(info_payload()))
    for index in range(3):
        inputs.scan(landed_record(tmp_path, 0, index, [index]))
    assert inputs.idx.index == 3


def test_a_record_without_landed_data_cannot_be_served(tmp_path):
    inputs = LandedInputs(Info.model_validate(info_payload()))
    with pytest.raises(NotLandedError):
        inputs.scan(InputRecord(file="/a.parquet"))

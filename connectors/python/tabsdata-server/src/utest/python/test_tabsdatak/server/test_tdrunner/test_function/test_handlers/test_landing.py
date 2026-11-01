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
import pyarrow.parquet as pq
import pytest

from tabsdatak.common._storage.landed import (
    TABSDATA_LANDING_FACTS_KEY,
    LandingFacts,
    SchemaOrigin,
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
    NotLandedError,
    SchemaMismatchError,
)
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def landing(root: Path) -> Landing:
    return Landing(Location(uri=(root / "landing").as_uri(), env_prefix="ROOT"))


def local(location: Location) -> Path:
    return Path(url2pathname(urlparse(location.uri).path))


def facts(location: Location) -> dict:
    metadata = pq.read_metadata(local(location)).metadata
    return json.loads(metadata[TABSDATA_LANDING_FACTS_KEY.encode()])


def test_typed_data_lands_with_its_own_types(tmp_path):
    frame = pl.LazyFrame(
        {
            "id": [1, 2, 3],
            "day": [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
        },
    )
    landed = landing(tmp_path).land(
        0,
        0,
        frame,
        metadata={"@query": "select 1"},
    )
    assert landed.origin is SchemaOrigin.DECLARED
    assert landed.rows == 3
    assert pl.read_parquet(local(landed.data)).equals(frame.collect())
    described = pl.read_parquet(local(landed.schema))
    assert described.height == 0
    assert described.schema == frame.collect_schema()
    assert facts(landed.schema) == {
        "rows": 3,
        "origin": "declared",
        "metadata": {"@query": "select 1"},
    }


def test_untyped_data_lands_as_strings_with_the_schema_polars_inferred(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("id,paid,day\n00123,true,2026-01-31\n7,False,2026-02-01\n")
    schema = pl.scan_csv(
        source,
        infer_schema_length=None,
        try_parse_dates=True,
    ).collect_schema()
    strings = pl.scan_csv(source, infer_schema=False)
    landed = landing(tmp_path).land(
        1,
        2,
        strings,
        schema=schema,
    )
    assert landed.origin is SchemaOrigin.INFERRED
    assert landed.data.uri.endswith("/landing/1.2.t")
    assert landed.schema.uri.endswith("/landing/1.2.s")
    data = pl.read_parquet(local(landed.data))
    assert data.to_dict(as_series=False) == {
        "id": ["00123", "7"],
        "paid": ["true", "False"],
        "day": ["2026-01-31", "2026-02-01"],
    }
    assert pl.read_parquet(local(landed.schema)).schema == pl.Schema(
        {"id": pl.Int64, "paid": pl.Boolean, "day": pl.Date},
    )
    assert facts(landed.schema) == {"rows": 2, "origin": "inferred", "metadata": None}


def test_an_empty_frame_lands_with_no_rows(tmp_path):
    frame = pl.LazyFrame(schema={"id": pl.Int64})
    landed = landing(tmp_path).land(0, 0, frame)
    assert landed.rows == 0
    assert pl.read_parquet(local(landed.data)).schema == frame.collect_schema()


def test_a_schema_must_name_the_data_columns(tmp_path):
    frame = pl.LazyFrame({"id": ["1"]})
    with pytest.raises(SchemaMismatchError, match=r"names columns \['code'\]"):
        landing(tmp_path).land(
            0,
            0,
            frame,
            schema=pl.Schema({"code": pl.Int64}),
        )


def test_a_schema_is_only_for_untyped_data(tmp_path):
    frame = pl.LazyFrame({"id": [1]})
    with pytest.raises(SchemaMismatchError, match=r"\['id'\] are typed"):
        landing(tmp_path).land(
            0,
            0,
            frame,
            schema=pl.Schema({"id": pl.Int64}),
        )


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


def test_typed_data_is_served_as_landed(tmp_path):
    frame = pl.LazyFrame({"id": [1, 2]})
    landed = landing(tmp_path).land(
        0,
        0,
        frame,
        metadata={"@query": "select 1"},
    )
    served, facts = landed.table.scan()
    assert served.collect().equals(frame.collect())
    assert facts == LandingFacts(
        rows=2,
        origin=SchemaOrigin.DECLARED,
        metadata={"@query": "select 1"},
    )


def test_untyped_data_is_served_with_its_inferred_schema(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("id,paid,day\n00123,true,31-01-2024\n7,False,01-02-2024\n")
    schema = pl.scan_csv(
        source,
        infer_schema_length=None,
        try_parse_dates=True,
    ).collect_schema()
    landed = landing(tmp_path).land(
        0,
        0,
        pl.scan_csv(source, infer_schema=False),
        schema=schema,
    )
    served, facts = landed.table.scan()
    assert served.collect().equals(pl.read_csv(source, schema=schema))
    assert facts.origin is SchemaOrigin.INFERRED
    assert facts.metadata is None


def landed_record(
    root: Path, position: int, index: int, values: list[int]
) -> InputRecord:
    landed = landing(root).land(
        position,
        index,
        pl.LazyFrame({"id": values}),
        metadata={"@part": index},
    )
    return InputRecord(location=landed.data, schema=landed.schema)


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

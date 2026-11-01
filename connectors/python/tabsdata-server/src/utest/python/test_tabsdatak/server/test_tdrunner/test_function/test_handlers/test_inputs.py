#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Info,
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._handlers.inputs import (
    Inputs,
    InputScanError,
    SingleSlotError,
)
from tabsdatak.server._tdrunner.function._handlers.landing import Landing
from tabsdatak.spi import TableMode
from tabsdatak.tableframe import TableFrame
from test_tabsdatak.server.test_tdrunner.test_function.conftest import info_payload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def write_sample_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).write_parquet(path)


def slot_entry(files: list[str]) -> InputEntry:
    return InputEntry(
        name="in_1",
        table_pos=0,
        tables=tuple(
            InputRecord(file=file, input_idx=index) for index, file in enumerate(files)
        ),
    )


def landed_entry(root: Path, chunks: list[list[int]]) -> InputEntry:
    landing = Landing(Location(uri=(root / "landing").as_uri(), env_prefix="ROOT"))
    records = []
    for index, values in enumerate(chunks):
        landed = landing.land(0, index, pl.LazyFrame({"id": values}))
        records.append(InputRecord(location=landed.data, schema=landed.schema))
    return InputEntry(name="in_1", table_pos=0, tables=tuple(records))


def inputs(*, publisher: bool = False) -> Inputs:
    return Inputs(Info.model_validate(info_payload()), publisher=publisher)


# --- scan -------------------------------------------------------------------


def test_scan_of_a_file(tmp_path):
    parquet_path = tmp_path / "x.parquet"
    write_sample_parquet(parquet_path)
    frame = inputs().scan(InputRecord(file=str(parquet_path)))
    assert isinstance(frame, pl.LazyFrame)


def test_scan_of_garbage_raises_input_scan(tmp_path):
    parquet_path = tmp_path / "x.parquet"
    parquet_path.write_bytes(b"not a parquet")
    with pytest.raises(InputScanError) as error:
        inputs().scan(InputRecord(file=str(parquet_path)))
    assert error.value.path == parquet_path.as_uri()


def test_scan_of_a_stored_location(tmp_path):
    parquet_path = tmp_path / "x.parquet"
    write_sample_parquet(parquet_path)
    record = InputRecord(
        location=Location(uri=parquet_path.as_uri(), env_prefix="ROOT")
    )
    frame = inputs().scan(record)
    assert frame.collect()["name"].to_list() == ["a", "b", "c"]


def test_scan_with_an_unusable_mount_raises_input_scan(monkeypatch):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__REGION", "eu")
    record = InputRecord(location=Location(uri="s3://b/x.parquet", env_prefix="ROOT"))
    with pytest.raises(InputScanError, match="s3://b/x.parquet") as error:
        inputs().scan(record)
    assert error.value.path == "s3://b/x.parquet"


def test_scan_of_a_missing_landed_file_raises_input_scan(tmp_path):
    missing = Location(uri=(tmp_path / "missing.s").as_uri(), env_prefix="ROOT")
    with pytest.raises(InputScanError) as error:
        inputs(publisher=True).scan(InputRecord(location=missing, schema=missing))
    assert error.value.path == missing.uri


# --- slot -------------------------------------------------------------------


def test_single_slot_of_zero_files_is_none():
    assert inputs().slot(slot_entry([]), TableMode.SINGLE) is None


def test_single_slot_of_one_file_is_a_table_frame(tmp_path):
    parquet_path = tmp_path / "x.parquet"
    write_sample_parquet(parquet_path)
    output = inputs().slot(slot_entry([str(parquet_path)]), TableMode.SINGLE)
    assert isinstance(output, TableFrame)


def test_single_slot_of_two_files_raises_single_slot(tmp_path):
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    write_sample_parquet(first)
    write_sample_parquet(second)
    with pytest.raises(SingleSlotError) as error:
        inputs().slot(slot_entry([str(first), str(second)]), TableMode.SINGLE)
    assert error.value.count == 2


def test_single_slot_of_a_chunked_publisher_input_is_one_table_frame(tmp_path):
    first, second = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).write_parquet(first)
    pl.DataFrame({"id": [None, None], "name": ["c", "d"]}).write_parquet(second)

    output = inputs(publisher=True).slot(
        slot_entry([str(first), str(second)]),
        TableMode.SINGLE,
    )

    assert isinstance(output, TableFrame)
    frame = output._to_polars_lf().collect()
    assert frame["name"].to_list() == ["a", "b", "c", "d"]
    assert frame["id"].to_list() == [1, 2, None, None]
    assert frame["id"].dtype == pl.Int64


def test_single_slot_chunks_of_unmergeable_types_are_strings(tmp_path):
    first, second = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pl.DataFrame(
        {"flag": [True], "tags": [[1, 2]], "score": [1]},
    ).write_parquet(first)
    pl.DataFrame(
        {"flag": [date(2026, 1, 2)], "tags": [None], "score": [2.5]},
        schema_overrides={"tags": pl.Int64},
    ).write_parquet(second)

    output = inputs(publisher=True).slot(
        slot_entry([str(first), str(second)]),
        TableMode.SINGLE,
    )

    frame = output._to_polars_lf().collect()
    assert frame.schema == pl.Schema(
        {"flag": pl.String, "tags": pl.String, "score": pl.Float64},
    )
    assert frame["flag"].to_list() == ["true", "2026-01-02"]
    assert frame["tags"].to_list() == ["[1,2]", None]
    assert frame["score"].to_list() == [1.0, 2.5]


def test_array_slot_is_a_list_of_table_frames(tmp_path):
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    write_sample_parquet(first)
    write_sample_parquet(second)
    output = inputs().slot(slot_entry([str(first), str(second)]), TableMode.ARRAY)
    assert isinstance(output, list)
    assert len(output) == 2
    assert all(isinstance(item, TableFrame) for item in output)


def test_array_slot_of_zero_files_is_an_empty_list():
    assert inputs().slot(slot_entry([]), TableMode.ARRAY) == []


def test_single_slot_of_landed_chunks_is_one_table_frame(tmp_path):
    output = inputs(publisher=True).slot(
        landed_entry(tmp_path, [[1, 2], [3]]),
        TableMode.SINGLE,
    )
    assert isinstance(output, TableFrame)
    assert output._to_polars_lf().collect()["id"].to_list() == [1, 2, 3]


def test_array_slot_of_landed_files_is_a_list(tmp_path):
    output = inputs(publisher=True).slot(
        landed_entry(tmp_path, [[1, 2], [3]]),
        TableMode.ARRAY,
    )
    assert isinstance(output, list)
    assert [frame._to_polars_lf().collect()["id"].to_list() for frame in output] == [
        [1, 2],
        [3],
    ]


def test_slot_of_a_missing_landed_file_raises_input_scan(tmp_path):
    missing = Location(uri=(tmp_path / "missing.s").as_uri(), env_prefix="ROOT")
    entry = InputEntry(
        name="in_1",
        table_pos=0,
        tables=(InputRecord(location=missing, schema=missing),),
    )
    with pytest.raises(InputScanError):
        inputs(publisher=True).slot(entry, TableMode.SINGLE)

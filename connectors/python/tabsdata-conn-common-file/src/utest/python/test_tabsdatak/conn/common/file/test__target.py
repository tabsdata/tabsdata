#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.common._error import CommonInitException
from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import DestFileFormat
from tabsdatak.conn.common.error import ConnException

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _target
from tabsdatak.conn.common.file._plugin import FilePluginErrors, WriterConf
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileCommonErrorCode

ERRORS = FilePluginErrors(
    read_failed=FileCommonErrorCode.FILE_COMMON_3,
    write_failed=FileCommonErrorCode.FILE_COMMON_8,
)

ROWS = pl.LazyFrame({
    "id": [1, 2],
    "name": ["a,b", None],
    "at": [
        datetime(2026, 1, 15, 11, 20, 30, tzinfo=timezone.utc),
        None,
    ],
}).with_columns(pl.col("at").dt.convert_time_zone("Europe/Madrid"))


@dataclass
class Ctx:
    execution_id: str = "exec"
    transaction_id: str = "trx"
    triggered_on: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)


def written(
    tmp_path: Path,
    paths: list[str],
    tables: list[pl.LazyFrame | None],
    file_format: DestFileFormat = DestFileFormat.AUTO,
    format_cfg: dict | None = None,
) -> Path:
    root = tmp_path / "out"
    _target.write_files(
        ctx=Ctx(),
        cfg=WriterConf(
            conn=None,
            paths=paths,
            file_format=file_format,
            format_cfg=format_cfg,
        ),
        store=FileStore(root.as_uri()),
        tables=tables,
        errors=ERRORS,
    )
    return root


@pytest.mark.parametrize(
    "path, expected",
    [
        ("out.csv", DestFileFormat.CSV),
        ("out.json", DestFileFormat.JSON),
        ("out.jsonl", DestFileFormat.JSON),
        ("out.ndjson", DestFileFormat.JSON),
        ("out.avro", DestFileFormat.AVRO),
        ("out.parquet", DestFileFormat.PARQUET),
        ("dir/Out.PQ", DestFileFormat.PARQUET),
    ],
)
def test_auto_takes_the_format_from_the_extension(path, expected):
    assert _target.format_of(DestFileFormat.AUTO, path) is expected


def test_an_explicit_format_is_taken_whatever_the_name():
    assert _target.format_of(DestFileFormat.CSV, "out.bin") is DestFileFormat.CSV


@pytest.mark.parametrize("path", ["out.bin", "out", "out.csv.gz", ".csv"])
def test_auto_refuses_an_unknown_extension(path):
    with pytest.raises(CommonInitException) as raised:
        _target.format_of(DestFileFormat.AUTO, path)
    assert raised.value.api_error is FileFormatErrorCode.FILE_FORMAT_4


def test_auto_refuses_paths_of_mixed_formats():
    with pytest.raises(CommonInitException) as raised:
        _target.formats(DestFileFormat.AUTO, ["a.csv", "b.parquet"])
    assert raised.value.api_error is FileFormatErrorCode.FILE_FORMAT_4


def test_eol_char_is_the_line_terminator():
    options = _target.csv_options({DestFileFormat.CSV: {"eol_char": "\r\n"}})
    assert options == {"line_terminator": "\r\n"}


def test_every_format_round_trips(tmp_path):
    for path in ["a.csv", "b.json", "c.parquet", "d.avro"]:
        root = written(tmp_path, [path], [ROWS])
    assert pl.read_csv(root / "a.csv")["id"].to_list() == [1, 2]
    assert pl.read_ndjson(root / "b.json")["name"].to_list() == ["a,b", None]
    assert pl.read_parquet(root / "c.parquet").equals(ROWS.collect())
    avro = pl.read_avro(root / "d.avro")
    assert avro["id"].to_list() == [1, 2]
    assert avro.schema["at"] == pl.Datetime("us", "UTC")


def test_one_write_refuses_paths_of_mixed_formats(tmp_path):
    with pytest.raises(CommonInitException) as raised:
        written(tmp_path, ["a.csv", "b.parquet"], [ROWS, ROWS])
    assert raised.value.api_error is FileFormatErrorCode.FILE_FORMAT_4


def test_folders_are_created_and_tokens_resolved(tmp_path):
    root = written(tmp_path, ["${TRX_ID}/deep/${EXEC_PLAN_ID}.csv"], [ROWS])
    assert (root / "trx" / "deep" / "exec.csv").is_file()


def test_a_write_replaces_the_previous_object(tmp_path):
    written(tmp_path, ["a.csv"], [ROWS])
    root = written(tmp_path, ["a.csv"], [ROWS.head(1)])
    assert pl.read_csv(root / "a.csv").height == 1


def test_every_csv_option_applies(tmp_path):
    options = {
        "separator": ";",
        "quote_char": "'",
        "eol_char": "\r\n",
        "include_header": False,
        "null_value": "NULL",
        "quote_style": "always",
        "datetime_format": "%Y/%m/%d %H:%M",
    }
    root = written(
        tmp_path,
        ["a.csv"],
        [ROWS],
        DestFileFormat.CSV,
        {DestFileFormat.CSV: options},
    )
    assert (root / "a.csv").read_bytes() == (
        b"'1';'a,b';'2026/01/15 11:20'\r\n'2';'NULL';'NULL'\r\n"
    )


def test_float_options_apply(tmp_path):
    root = written(
        tmp_path,
        ["a.csv"],
        [pl.LazyFrame({"f": [1.23456]})],
        DestFileFormat.CSV,
        {DestFileFormat.CSV: {"float_precision": 2, "include_header": False}},
    )
    assert (root / "a.csv").read_text(encoding="utf-8") == "1.23\n"


def test_csv_and_json_lines_write_zoned_times_in_utc(tmp_path):
    written(tmp_path, ["a.csv"], [ROWS])
    root = written(tmp_path, ["b.ndjson"], [ROWS])
    assert "2026-01-15T11:20:30.000000+0000" in (root / "a.csv").read_text(
        encoding="utf-8"
    )
    first = json.loads((root / "b.ndjson").read_text(encoding="utf-8").splitlines()[0])
    assert first["at"] == "2026-01-15T11:20:30+00:00"


def test_json_lines_write_null_values(tmp_path):
    root = written(tmp_path, ["b.ndjson"], [ROWS])
    second = json.loads((root / "b.ndjson").read_text(encoding="utf-8").splitlines()[1])
    assert second == {"id": 2, "name": None, "at": None}


def test_a_failed_write_raises_the_connector_code_and_leaves_nothing(tmp_path):
    broken = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").str.to_integer())
    with pytest.raises(ConnException) as raised:
        written(tmp_path, ["a.csv", "b.csv"], [ROWS, broken])
    assert raised.value.api_error is FileCommonErrorCode.FILE_COMMON_8
    assert raised.value.extra["target"].endswith("/b.csv")
    assert sorted(path.name for path in (tmp_path / "out").iterdir()) == ["a.csv"]


def test_a_failed_avro_write_leaves_nothing(tmp_path):
    broken = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").str.to_integer())
    with pytest.raises(ConnException):
        written(tmp_path, ["a.avro"], [broken])
    assert list((tmp_path / "out").iterdir()) == []


def test_tables_and_paths_must_pair(tmp_path):
    with pytest.raises(ConnException) as raised:
        written(tmp_path, ["a.csv", "b.csv"], [ROWS])
    assert raised.value.api_error is FileCommonErrorCode.FILE_COMMON_16
    assert raised.value.extra == {"tables": 1, "paths": 2}


def test_an_empty_table_writes_its_columns(tmp_path):
    empty = pl.LazyFrame(schema={"id": pl.Int64, "name": pl.String})
    written(tmp_path, ["a.csv"], [empty])
    root = written(tmp_path, ["b.avro"], [empty])
    assert (root / "a.csv").read_text(encoding="utf-8") == "id,name\n"
    assert pl.read_avro(io.BytesIO((root / "b.avro").read_bytes())).columns == [
        "id",
        "name",
    ]


def test_a_none_table_leaves_its_path_untouched(tmp_path):
    written(tmp_path, ["a.csv", "b.csv", "c.csv"], [ROWS, ROWS, ROWS])
    root = written(
        tmp_path, ["a.csv", "b.csv", "c.csv"], [ROWS.head(1), None, ROWS.head(1)]
    )
    assert pl.read_csv(root / "a.csv").height == 1
    assert pl.read_csv(root / "b.csv").height == 2
    assert pl.read_csv(root / "c.csv").height == 1


def test_a_none_table_creates_no_file(tmp_path):
    root = written(tmp_path, ["a.csv", "b.csv"], [None, ROWS])
    assert sorted(path.name for path in root.iterdir()) == ["b.csv"]


def test_only_none_tables_write_nothing(tmp_path):
    root = written(tmp_path, ["a.csv", "${TRX_ID}/b.csv"], [None, None])
    assert not root.exists()


def test_paths_of_mixed_formats_are_refused_even_for_a_none_table(tmp_path):
    with pytest.raises(CommonInitException) as raised:
        written(tmp_path, ["a.csv", "b.parquet"], [ROWS, None])
    assert raised.value.api_error is FileFormatErrorCode.FILE_FORMAT_4


def test_tables_and_paths_must_pair_with_none_tables(tmp_path):
    with pytest.raises(ConnException) as raised:
        written(tmp_path, ["a.csv"], [None, None])
    assert raised.value.api_error is FileCommonErrorCode.FILE_COMMON_16

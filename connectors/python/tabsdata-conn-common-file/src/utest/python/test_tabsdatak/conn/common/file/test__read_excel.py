#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import openpyxl
import polars as pl
import pytest

from tabsdatak.common._fileformat.taxonomy import EXCEL_SHEET_KEY

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._read._excel import Excel
from test_tabsdatak.conn.common.file.landed import Source, land

pytestmark = [pytest.mark.unit]

SHEETS = {
    "First": [["tag"], ["from-sheet-1"]],
    "Second": [["tag"], ["from-sheet-2"]],
}


def workbook(sheets: dict[str, list[list]], hidden: tuple[str, ...] = ()) -> bytes:
    book = openpyxl.Workbook()
    book.remove(book.active)
    for name, rows in sheets.items():
        sheet = book.create_sheet(name)
        for row in rows:
            sheet.append(row)
        if name in hidden:
            sheet.sheet_state = "hidden"
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def served(tmp_path, content: bytes, options: dict | None = None, **kwargs):
    return land(Excel(options, **kwargs).read(Source(content)), tmp_path).served


def test_no_sheet_reads_the_first(tmp_path):
    assert served(tmp_path, workbook(SHEETS)).to_dicts() == [{"tag": "from-sheet-1"}]


def test_the_sheet_is_counted_from_one_hidden_sheets_included(tmp_path):
    content = workbook(SHEETS, hidden=("First",))
    frame = served(tmp_path, content, {EXCEL_SHEET_KEY: 2})
    assert frame.to_dicts() == [{"tag": "from-sheet-2"}]


@pytest.mark.parametrize(
    "sheet, reason",
    [(0, "start at 1"), (3, "out of range: the workbook has 2 sheet")],
)
def test_a_sheet_the_workbook_has_not_is_refused(sheet, reason):
    with pytest.raises(ValueError, match=reason):
        Excel({EXCEL_SHEET_KEY: sheet}).read(Source(workbook(SHEETS)))


def test_without_a_header_the_columns_are_numbered(tmp_path):
    content = workbook({"Data": [["id", "name"], ["a", "alice"], ["b", "bob"]]})
    assert served(tmp_path, content, {"has_header": False}).to_dicts() == [
        {"column_1": "id", "column_2": "name"},
        {"column_1": "a", "column_2": "alice"},
        {"column_1": "b", "column_2": "bob"},
    ]


def test_columns_take_the_types_their_cells_declare(tmp_path):
    rows = [
        ["n", "ratio", "label", "sparse", "when", "code"],
        [1, 1.5, "a", 10, datetime(2024, 1, 2, 3, 4, 5), "007"],
        [2, 2.0, "b", None, datetime(2024, 2, 3), "042"],
    ]
    frame = served(tmp_path, workbook({"Data": rows}))
    assert dict(frame.schema) == {
        "n": pl.Int64,
        "ratio": pl.Float64,
        "label": pl.String,
        "sparse": pl.Int64,
        "when": pl.Datetime("us"),
        "code": pl.String,
    }
    assert frame.to_dicts() == [
        {
            "n": 1,
            "ratio": 1.5,
            "label": "a",
            "sparse": 10,
            "when": datetime(2024, 1, 2, 3, 4, 5),
            "code": "007",
        },
        {
            "n": 2,
            "ratio": 2.0,
            "label": "b",
            "sparse": None,
            "when": datetime(2024, 2, 3),
            "code": "042",
        },
    ]


def test_a_column_mixing_numbers_and_text_is_text(tmp_path):
    frame = served(tmp_path, workbook({"Data": [["v"], [1], ["x"], [2.5]]}))
    assert frame.to_dicts() == [{"v": "1"}, {"v": "x"}, {"v": "2.5"}]


def test_rows_are_cut_into_batches(tmp_path):
    rows = [["n"]] + [[value] for value in range(10)]
    frame = served(tmp_path, workbook({"Data": rows}), batch_rows=3)
    assert frame["n"].to_list() == list(range(10))


def test_a_file_that_is_not_a_workbook_is_refused_and_closed():
    source = Source(b"id,name\n1,alice\n")
    with pytest.raises(ValueError, match="excel"):
        Excel().read(source)
    assert source.streams[0].closed


def test_the_file_is_closed_once_read(tmp_path):
    source = Source(workbook(SHEETS))
    land(Excel().read(source), tmp_path)
    assert len(source.streams) == 1
    assert source.streams[0].closed


DEADLOCK_SECONDS = 60
ROWS_BEYOND_THE_READ_BUFFER = 50000
LAND_WORKBOOK = Path(__file__).parent / "children" / "land_workbook.py"


def test_a_workbook_larger_than_the_read_buffer_lands_in_small_batches(tmp_path):
    rows = [["n", "text", "ratio", "when", "flag", "code", "label"]] + [
        [
            value,
            f"row-{value}",
            value * 0.25,
            datetime(2024, 1, 1) + timedelta(seconds=value),
            value % 2 == 0,
            f"{value:06d}",
            f"label-{value % 1000}",
        ]
        for value in range(ROWS_BEYOND_THE_READ_BUFFER)
    ]
    (tmp_path / "big.xlsx").write_bytes(workbook({"Data": rows}))
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    landed = subprocess.run(
        [
            sys.executable,
            str(LAND_WORKBOOK),
            tmp_path.as_uri(),
            "big.xlsx",
            str(tmp_path / "landed.parquet"),
        ],
        capture_output=True,
        encoding="utf-8",
        env=environment,
        timeout=DEADLOCK_SECONDS,
        check=True,
    )
    assert landed.stdout.split() == [str(ROWS_BEYOND_THE_READ_BUFFER)]

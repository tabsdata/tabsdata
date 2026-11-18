#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone

import openpyxl
import polars as pl
import pytest
from tabsdatax.itest.fixture.transporter import no_transporter  # noqa: F401

from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.conn.common.constants import SRC_META_DROP_KEY

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _source

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._plugin import FilePluginErrors, ReaderConf

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.constants import FILE_META_PATH
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.spi import FrameMetadata, LazyFrameInput

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_transporter")]

ERRORS = FilePluginErrors(
    read_failed=FileCommonErrorCode.FILE_COMMON_3,
    write_failed=FileCommonErrorCode.FILE_COMMON_8,
)

INITIAL = datetime(2000, 1, 1, tzinfo=timezone.utc)


class Src:
    def __init__(self, paths: list[str], file_format=SrcFileFormat.AUTO) -> None:
        self.paths = paths
        self.format = file_format
        self.format_cfg = None
        self.initial_last_modified = None
        self.src_cfg = None
        self.purge = False
        self.include_compressed = False


class Context:
    def __init__(self) -> None:
        self.attrs: dict[str, object] = {}

    def get_attr(self, key: str) -> object:
        return self.attrs.get(key)

    def set_attr(self, key: str, value: object) -> None:
        self.attrs[key] = value


def origin(_store: FileStore, path: str) -> str:
    return f"s3://bucket/{path}"


def read(tmp_path, cfg: ReaderConf, ctx: Context | None = None):
    return [
        list(slot)
        for slot in _source.read_files(
            ctx=ctx or Context(),
            cfg=cfg,
            store=FileStore(tmp_path.as_uri()),
            origin=origin,
            errors=ERRORS,
        )
    ]


def unknown(table: LazyFrameInput) -> bool:
    return (
        isinstance(table.metadata, FrameMetadata)
        and not table.metadata.known
        and table.frame.collect().shape == (0, 0)
    )


def path_of(table: LazyFrameInput) -> str | None:
    metadata = table.metadata() if callable(table.metadata) else table.metadata
    return None if metadata is None else metadata.columns[FILE_META_PATH]


def workbook(target, sheets: int) -> None:
    book = openpyxl.Workbook()
    book.remove(book.active)
    for number in range(1, sheets + 1):
        sheet = book.create_sheet(f"s{number}")
        sheet.append(["sheet"])
        sheet.append([number])
    buffer = io.BytesIO()
    book.save(buffer)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(buffer.getvalue())


def sheet_read(table: LazyFrameInput) -> int:
    return table.frame.collect()["sheet"].cast(pl.Int64).item()


def test_each_slot_reads_its_own_worksheet_and_names_it_in_the_path(tmp_path):
    workbook(tmp_path / "2026" / "q1.xlsx", 1)
    workbook(tmp_path / "2026" / "q2.xlsx", 3)
    cfg = ReaderConf.from_src(
        None, Src(["2026/q2.xlsx#2", "2026/q2.xlsx#3", "2026/q1.xlsx"])
    )
    slots = read(tmp_path, cfg)
    assert [sheet_read(table) for (table,) in slots] == [2, 3, 1]
    assert [path_of(table) for (table,) in slots] == [
        "s3://bucket/2026/q2.xlsx#2",
        "s3://bucket/2026/q2.xlsx#3",
        "s3://bucket/2026/q1.xlsx",
    ]


def test_every_globbed_workbook_gets_its_own_path_and_the_slots_worksheet(tmp_path):
    workbook(tmp_path / "2026" / "north-sales.xlsx", 4)
    workbook(tmp_path / "2026" / "south-sales.xlsx", 4)
    cfg = ReaderConf.from_src(None, Src(["2026/*-sales.xlsx#4"]))
    (slot,) = read(tmp_path, cfg)
    assert sorted(path_of(table) for table in slot) == [
        "s3://bucket/2026/north-sales.xlsx#4",
        "s3://bucket/2026/south-sales.xlsx#4",
    ]
    assert [sheet_read(table) for table in slot] == [4, 4]


@pytest.mark.parametrize("name", ["rows.csv", "2026/q2.xlsx#2"])
def test_dropping_metadata_leaves_no_path(tmp_path, name):
    (tmp_path / "rows.csv").write_text("a\n1\n", encoding="utf-8")
    workbook(tmp_path / "2026" / "q2.xlsx", 2)
    src = Src([name])
    src.src_cfg = {SRC_META_DROP_KEY: True}
    ((table,),) = read(tmp_path, ReaderConf.from_src(None, src))
    metadata = table.metadata() if callable(table.metadata) else table.metadata
    assert metadata is None or FILE_META_PATH not in (metadata.columns or {})


def test_without_an_initial_lastmod_no_cursor_is_kept(tmp_path):
    (tmp_path / "rows.csv").write_text("a\n1\n", encoding="utf-8")
    ctx = Context()
    read(tmp_path, ReaderConf.from_src(None, Src(["rows.csv"])), ctx)
    read(tmp_path, ReaderConf.from_src(None, Src(["rows.csv"])), ctx)
    assert ctx.attrs == {}


def test_each_slot_keeps_its_own_cursor_and_skips_what_it_took(tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n", encoding="utf-8")
    (tmp_path / "b.csv").write_text("b\n2\n", encoding="utf-8")
    src = Src(["a.csv", "b.csv"])
    src.initial_last_modified = INITIAL
    ctx = Context()
    first = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert [len(slot) for slot in first] == [1, 1]
    assert sorted(ctx.attrs) == ["file-lastmod.slot0", "file-lastmod.slot1"]
    again = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert [[unknown(table) for table in slot] for slot in again] == [[True], [True]]


def csv(path, when: datetime):
    path.write_text("id\n1\n", encoding="utf-8")
    os.utime(path, (when.timestamp(), when.timestamp()))
    return path


def seeded(tmp_path, purge: bool):
    a = csv(tmp_path / "a.csv", INITIAL + timedelta(hours=1))
    b = csv(tmp_path / "b.csv", INITIAL + timedelta(hours=2))
    src = Src(["*.csv"])
    src.initial_last_modified = INITIAL
    src.purge = purge
    return src, a, b


def test_purge_deletes_what_a_prior_run_took(tmp_path):
    src, a, b = seeded(tmp_path, purge=True)
    ctx = Context()
    (first,) = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert len(first) == 2
    assert a.exists() and b.exists()
    ((second,),) = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert unknown(second)
    assert not a.exists() and not b.exists()


def test_purge_leaves_the_file_it_is_reading(tmp_path):
    src, a, b = seeded(tmp_path, purge=True)
    ctx = Context()
    read(tmp_path, ReaderConf.from_src(None, src), ctx)
    fresh = csv(tmp_path / "c.csv", INITIAL + timedelta(hours=3))
    (second,) = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert len(second) == 1
    assert fresh.exists()
    assert not a.exists() and not b.exists()


def test_purge_never_deletes_below_the_initial_lastmod(tmp_path):
    src, a, b = seeded(tmp_path, purge=True)
    stale = csv(tmp_path / "stale.csv", INITIAL - timedelta(days=1))
    ctx = Context()
    (first,) = read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert len(first) == 2
    read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert stale.exists()
    assert not a.exists() and not b.exists()


def test_without_purge_nothing_is_deleted(tmp_path):
    src, a, b = seeded(tmp_path, purge=False)
    ctx = Context()
    read(tmp_path, ReaderConf.from_src(None, src), ctx)
    read(tmp_path, ReaderConf.from_src(None, src), ctx)
    assert a.exists() and b.exists()


def test_a_path_matching_no_file_reads_one_empty_table_of_unknown_columns(tmp_path):
    (tmp_path / "a.csv").write_text("a\n1\n", encoding="utf-8")
    [[table]] = read(tmp_path, ReaderConf.from_src(None, Src(["*.json"])))
    assert unknown(table)
    assert path_of(table) == "s3://bucket/*.json"


def test_a_path_matching_no_file_without_metadata_has_no_columns_to_add(tmp_path):
    src = Src(["*.json"])
    src.src_cfg = {SRC_META_DROP_KEY: True}
    [[table]] = read(tmp_path, ReaderConf.from_src(None, src))
    assert unknown(table)
    assert table.metadata.columns is None

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import gzip
import io
import tarfile
import zipfile

import polars as pl
import pytest

from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.common._storage.scratch import ScratchFiles

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _source

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._plugin import ReaderConf

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._select import archive, plain, wrapper

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileRuntimeException
from test_tabsdatak.conn.common.file.landed import land
from test_tabsdatak.conn.common.file.test__source_codecs import (
    ERRORS,
    FRAME,
    Context,
    avro_bytes,
    ndjson_bytes,
    parquet_bytes,
    xlsx_bytes,
)

pytestmark = [pytest.mark.unit]


def zipped(*members: tuple[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as sink:
        for name, content in members:
            sink.writestr(name, content)
    return buffer.getvalue()


def tarred(*members: tuple[str, bytes | None]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as sink:
        for name, content in members:
            info = tarfile.TarInfo(name)
            if content is None:
                info.type = tarfile.DIRTYPE
                sink.addfile(info)
                continue
            info.size = len(content)
            sink.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


PACKERS = {".zip": zipped, ".tar": tarred}

CONTENTS = {
    SrcFileFormat.AVRO: ("in.avro", avro_bytes),
    SrcFileFormat.CSV: ("in.csv", lambda: FRAME.write_csv().encode()),
    SrcFileFormat.EXCEL: ("in.xlsx", xlsx_bytes),
    SrcFileFormat.JSON: ("in.ndjson", ndjson_bytes),
    SrcFileFormat.PARQUET: ("in.parquet", parquet_bytes),
}


def served(
    tmp_path,
    path: str,
    file_format: SrcFileFormat = SrcFileFormat.AUTO,
    include_compressed: bool = True,
) -> list[pl.DataFrame]:
    scratch_root = tmp_path / "scratch"
    scratch_root.mkdir(exist_ok=True)
    scratch = ScratchFiles(scratch_root.as_uri(), {})
    cfg = ReaderConf(
        conn=None,
        paths=[path],
        file_format=file_format,
        include_compressed=include_compressed,
    )
    (slot,) = _source.read_files(
        ctx=Context(scratch),
        cfg=cfg,
        store=FileStore((tmp_path / "data").as_uri()),
        origin=lambda store, name: store.uri(name),
        errors=ERRORS,
    )
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    try:
        return [land(table, landing).served for table in slot]
    finally:
        scratch.clear()


def stage(tmp_path, name: str, content: bytes) -> None:
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    (data / name).write_bytes(content)


@pytest.mark.parametrize("extension", sorted(PACKERS))
@pytest.mark.parametrize("file_format", sorted(CONTENTS, key=lambda f: f.value))
def test_each_format_lands_from_a_single_member_archive(
    tmp_path, extension, file_format
):
    name, content = CONTENTS[file_format]
    stage(tmp_path, f"{name}{extension}", PACKERS[extension](("member", content())))
    (frame,) = served(tmp_path, f"{name}{extension}")
    assert frame.equals(FRAME)


@pytest.mark.parametrize("extension", sorted(PACKERS))
def test_the_outer_name_decides_the_format_not_the_members(tmp_path, extension):
    content = FRAME.write_csv().encode()
    stage(tmp_path, f"in.csv{extension}", PACKERS[extension](("data.json", content)))
    (frame,) = served(tmp_path, f"in.csv{extension}")
    assert frame.equals(FRAME)


def test_a_log_member_is_named_by_the_archive(tmp_path):
    stage(tmp_path, "app.log.zip", zipped(("x", b"first\nsecond\n")))
    (frame,) = served(tmp_path, "app.log.zip")
    assert frame["message"].to_list() == ["first", "second"]


@pytest.mark.parametrize(
    "extension, members, reason",
    [
        (".zip", [("a", b"x\n1\n"), ("b", b"x\n2\n")], "it holds 2 entries"),
        (".zip", [], "it is empty"),
        (".zip", [("dir/", b"")], "is not a file"),
        (".tar", [("a", b"x\n1\n"), ("b", b"x\n2\n")], "more than one entry"),
        (".tar", [], "it is empty"),
        (".tar", [("dir", None)], "is not a file"),
    ],
    ids=["zip two", "zip empty", "zip folder", "tar two", "tar empty", "tar folder"],
)
def test_an_archive_not_holding_exactly_one_file_fails_the_run(
    tmp_path, extension, members, reason
):
    stage(tmp_path, f"in.csv{extension}", PACKERS[extension](*members))
    with pytest.raises(
        (FileRuntimeException, pl.exceptions.ComputeError), match=reason
    ):
        served(tmp_path, f"in.csv{extension}")


@pytest.mark.parametrize("extension", sorted(PACKERS))
def test_a_second_member_fails_a_random_access_format_too(tmp_path, extension):
    pair = [("a", parquet_bytes()), ("b", parquet_bytes())]
    stage(tmp_path, f"in.parquet{extension}", PACKERS[extension](*pair))
    with pytest.raises(
        (FileRuntimeException, pl.exceptions.ComputeError), match="exactly one file"
    ):
        served(tmp_path, f"in.parquet{extension}")


def test_an_archive_that_is_not_one_fails_the_run(tmp_path):
    stage(tmp_path, "in.csv.zip", b"not a zip at all")
    with pytest.raises(
        (FileRuntimeException, pl.exceptions.ComputeError), match="not a readable zip"
    ):
        served(tmp_path, "in.csv.zip")


@pytest.mark.parametrize(
    "name, content",
    [
        ("in.csv.tar.gz", gzip.compress(tarred(("a", b"x\n1\n")))),
        ("in.csv.gz.zip", zipped(("a", gzip.compress(b"x\n1\n")))),
        ("in.csv.gz.zst", b"irrelevant"),
    ],
    ids=["tar behind gzip", "gzip inside zip", "two codecs"],
)
def test_a_file_wrapped_twice_fails_before_it_is_read(tmp_path, name, content):
    stage(tmp_path, name, content)
    with pytest.raises(FileRuntimeException, match="only one"):
        served(tmp_path, name, SrcFileFormat.CSV)


def test_archives_are_selected_like_compressed_files(tmp_path):
    stage(tmp_path, "a.csv", FRAME.write_csv().encode())
    stage(tmp_path, "b.csv.zip", zipped(("b", FRAME.write_csv().encode())))
    stage(tmp_path, "c.csv.tar", tarred(("c", FRAME.write_csv().encode())))
    assert len(served(tmp_path, "*.csv", include_compressed=False)) == 1
    assert len(served(tmp_path, "*.csv", include_compressed=True)) == 3


@pytest.mark.parametrize(
    "name, wrapped, kind, bare",
    [
        ("a.csv.zip", ".zip", ".zip", "a.csv"),
        ("a.csv.TAR", ".tar", ".tar", "a.csv"),
        ("a.csv.gz", ".gz", None, "a.csv"),
        ("a.zip.csv", None, None, "a.zip.csv"),
    ],
)
def test_archive_extensions_are_wrappers(name, wrapped, kind, bare):
    assert wrapper(name) == wrapped
    assert archive(name) == kind
    assert plain(name) == bare

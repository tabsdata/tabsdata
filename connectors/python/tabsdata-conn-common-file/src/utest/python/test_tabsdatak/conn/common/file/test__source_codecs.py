#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import gzip
import io
import lzma

import polars as pl
import pytest
import zstandard

from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.conn.common.error import ConnException

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _source

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._plugin import FilePluginErrors, ReaderConf

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from test_tabsdatak.conn.common.file.landed import land

pytestmark = [pytest.mark.unit]

ERRORS = FilePluginErrors(
    binary_not_found=FileCommonErrorCode.FILE_COMMON_37,
    read_failed=FileCommonErrorCode.FILE_COMMON_37,
    write_failed=FileCommonErrorCode.FILE_COMMON_37,
    report_invalid=FileCommonErrorCode.FILE_COMMON_37,
)

FRAME = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})


class Context:
    def __init__(self) -> None:
        self.attrs: dict[str, object] = {}

    def get_attr(self, key: str) -> object:
        return self.attrs.get(key)

    def set_attr(self, key: str, value: object) -> None:
        self.attrs[key] = value


def avro_bytes() -> bytes:
    buffer = io.BytesIO()
    FRAME.write_avro(buffer)
    return buffer.getvalue()


def ndjson_bytes() -> bytes:
    buffer = io.BytesIO()
    FRAME.write_ndjson(buffer)
    return buffer.getvalue()


def served(tmp_path, path: str, file_format: SrcFileFormat) -> list[pl.DataFrame]:
    cfg = ReaderConf(
        conn=None, paths=[path], file_format=file_format, include_compressed=True
    )
    (slot,) = _source.read_files(
        ctx=Context(),
        cfg=cfg,
        store=FileStore(tmp_path.as_uri()),
        origin=lambda store, name: store.uri(name),
        errors=ERRORS,
    )
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    return [land(table, landing).served for table in slot]


@pytest.fixture
def codecs_on(monkeypatch):
    monkeypatch.setattr(_source, "CODECS_ENABLED", True)


@pytest.mark.parametrize(
    "name, content, file_format, expected",
    [
        ("in.csv.gz", gzip.compress(FRAME.write_csv().encode()), SrcFileFormat.CSV, FRAME),
        (
            "in.ndjson.zst",
            zstandard.ZstdCompressor().compress(ndjson_bytes()),
            SrcFileFormat.JSON,
            FRAME,
        ),
        ("in.avro.xz", lzma.compress(avro_bytes()), SrcFileFormat.AVRO, FRAME),
        (
            "app.log.gz",
            gzip.compress(b"first\nsecond\n"),
            SrcFileFormat.LOG,
            pl.DataFrame({"file": ["app.log.gz"] * 2, "message": ["first", "second"]}),
        ),
    ],
    ids=["csv gzip", "ndjson zstd", "avro xz", "log gzip"],
)
def test_a_compressed_file_lands_as_its_plain_form(
    tmp_path, codecs_on, name, content, file_format, expected
):
    (tmp_path / name).write_bytes(content)
    (frame,) = served(tmp_path, name, file_format)
    assert frame.equals(expected)


def test_auto_takes_the_format_inside_the_compression_extension(tmp_path, codecs_on):
    (tmp_path / "in.csv.gz").write_bytes(gzip.compress(FRAME.write_csv().encode()))
    (frame,) = served(tmp_path, "*.csv.gz", SrcFileFormat.AUTO)
    assert frame.equals(FRAME)


@pytest.mark.parametrize(
    "name, file_format",
    [("in.parquet.gz", SrcFileFormat.PARQUET), ("in.xlsx.gz", SrcFileFormat.EXCEL)],
    ids=["parquet", "excel"],
)
def test_a_compressed_file_read_in_place_is_refused(
    tmp_path, codecs_on, name, file_format
):
    (tmp_path / name).write_bytes(gzip.compress(b"anything"))
    with pytest.raises(ConnException, match="cannot be read compressed yet"):
        served(tmp_path, name, file_format)


def test_compressed_files_are_refused_while_codecs_are_off(tmp_path):
    (tmp_path / "in.csv.gz").write_bytes(gzip.compress(FRAME.write_csv().encode()))
    with pytest.raises(ConnException, match="cannot be read yet"):
        served(tmp_path, "in.csv.gz", SrcFileFormat.CSV)

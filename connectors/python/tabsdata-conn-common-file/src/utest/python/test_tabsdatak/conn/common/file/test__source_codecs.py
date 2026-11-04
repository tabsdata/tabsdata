#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import gzip
import io
import lzma
import uuid

import openpyxl
import polars as pl
import pytest
import zstandard
from tabsdatax.itest.fixture.backends.azurebloblocal import AzureBlobLocalDomain
from tabsdatax.itest.fixture.backends.gcslocal import GcsLocalDomain
from tabsdatax.itest.fixture.backends.s3local import S3LocalDomain
from tabsdatax.itest.fixture.emulators import EMULATORS, emulated

from tabsdatak.common._fileformat.taxonomy import SrcFileFormat
from tabsdatak.common._storage.scratch import ScratchFiles

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
    binary_not_found=FileCommonErrorCode.FILE_COMMON_1,
    read_failed=FileCommonErrorCode.FILE_COMMON_1,
    write_failed=FileCommonErrorCode.FILE_COMMON_1,
    report_invalid=FileCommonErrorCode.FILE_COMMON_1,
)

FRAME = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})


class Context:
    def __init__(self, scratch: ScratchFiles | None = None) -> None:
        self.attrs: dict[str, object] = {}
        self._scratch = scratch

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


def served(
    tmp_path,
    path: str,
    file_format: SrcFileFormat,
    scratch: ScratchFiles | None = None,
) -> list[pl.DataFrame]:
    cfg = ReaderConf(
        conn=None, paths=[path], file_format=file_format, include_compressed=True
    )
    (slot,) = _source.read_files(
        ctx=Context(scratch),
        cfg=cfg,
        store=FileStore(tmp_path.as_uri()),
        origin=lambda store, name: store.uri(name),
        errors=ERRORS,
    )
    landing = tmp_path / "landing"
    landing.mkdir(exist_ok=True)
    return [land(table, landing).served for table in slot]


@pytest.mark.parametrize(
    "name, content, file_format, expected",
    [
        (
            "in.csv.gz",
            gzip.compress(FRAME.write_csv().encode()),
            SrcFileFormat.CSV,
            FRAME,
        ),
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
    tmp_path, name, content, file_format, expected
):
    (tmp_path / name).write_bytes(content)
    (frame,) = served(tmp_path, name, file_format)
    assert frame.equals(expected)


def test_auto_takes_the_format_inside_the_compression_extension(tmp_path):
    (tmp_path / "in.csv.gz").write_bytes(gzip.compress(FRAME.write_csv().encode()))
    (frame,) = served(tmp_path, "*.csv.gz", SrcFileFormat.AUTO)
    assert frame.equals(FRAME)


def parquet_bytes() -> bytes:
    buffer = io.BytesIO()
    FRAME.write_parquet(buffer)
    return buffer.getvalue()


def xlsx_bytes() -> bytes:
    book = openpyxl.Workbook()
    sheet = book.active
    sheet.append(FRAME.columns)
    for row in FRAME.iter_rows():
        sheet.append(list(row))
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


@emulated
@pytest.mark.parametrize("emulator", EMULATORS)
@pytest.mark.parametrize(
    "name, content, file_format",
    [
        ("in.parquet.gz", gzip.compress(parquet_bytes()), SrcFileFormat.PARQUET),
        (
            "in.xlsx.zst",
            zstandard.ZstdCompressor().compress(xlsx_bytes()),
            SrcFileFormat.EXCEL,
        ),
    ],
    ids=["parquet gzip", "excel zstd"],
)
def test_a_compressed_file_read_in_place_lands_through_the_scratch(
    domain_azurebloblocal: AzureBlobLocalDomain,
    domain_gcslocal: GcsLocalDomain,
    domain_s3local: S3LocalDomain,
    tmp_path,
    emulator,
    name,
    content,
    file_format,
):
    domain = locals()[f"domain_{emulator}"]
    root = f"{domain.uri.rstrip('/')}/scratch-{uuid.uuid4().hex}"
    options = dict(domain.storage_options)
    scratch = ScratchFiles(root, options)
    (tmp_path / name).write_bytes(content)
    try:
        (frame,) = served(tmp_path, name, file_format, scratch)
        assert frame.equals(FRAME)
        (staged,) = FileStore(root, options).listed("")
        assert staged.name.endswith(name.rsplit(".", 1)[0])
    finally:
        scratch.clear()

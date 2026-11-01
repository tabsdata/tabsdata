#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest
import zstandard

from tabsdatak.common._storage.transfer import StorageLocation
from tabsdatak.server._tdrunner.function._handlers.bundle import (
    BundleArchive,
    DownloadError,
    ExtractError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def write_bundle(path: Path, files: dict[str, bytes]) -> BundleArchive:
    with open(path, "wb") as raw:
        with zstandard.ZstdCompressor().stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, content in files.items():
                    member = tarfile.TarInfo(name)
                    member.size = len(content)
                    tar.addfile(member, io.BytesIO(content))
    return BundleArchive(path)


def test_a_downloaded_bundle_extracts_into_its_folder(tmp_path):
    source = tmp_path / "remote" / "bundle.tar.zst"
    source.parent.mkdir()
    write_bundle(source, {"code/main.py": b"print(1)"})
    archive = BundleArchive(tmp_path / "bundle.tar.zst")
    archive.download(StorageLocation(uri=source.as_uri(), options={}))
    archive.extract(tmp_path / "bundle")
    assert (tmp_path / "bundle" / "code" / "main.py").read_bytes() == b"print(1)"


def test_a_missing_bundle_is_a_download_error(tmp_path):
    archive = BundleArchive(tmp_path / "bundle.tar.zst")
    missing = (tmp_path / "missing.tar.zst").as_uri()
    with pytest.raises(DownloadError):
        archive.download(StorageLocation(uri=missing, options={}))


def test_every_file_extracts_in_place(tmp_path):
    archive = write_bundle(
        tmp_path / "bundle.tar.zst",
        {"a.txt": b"hello", "subdir/b.txt": b"world"},
    )
    archive.extract(tmp_path / "out")
    assert (tmp_path / "out" / "a.txt").read_bytes() == b"hello"
    assert (tmp_path / "out" / "subdir" / "b.txt").read_bytes() == b"world"


def test_a_missing_folder_is_created_with_its_parents(tmp_path):
    archive = write_bundle(tmp_path / "bundle.tar.zst", {"a.txt": b"hi"})
    folder = tmp_path / "deep" / "nested" / "out"
    archive.extract(folder)
    assert (folder / "a.txt").read_bytes() == b"hi"


def test_an_existing_folder_keeps_its_content(tmp_path):
    archive = write_bundle(tmp_path / "bundle.tar.zst", {"new.txt": b"new"})
    folder = tmp_path / "out"
    folder.mkdir()
    (folder / "preexisting.txt").write_text("keep me")
    archive.extract(folder)
    assert (folder / "new.txt").read_bytes() == b"new"
    assert (folder / "preexisting.txt").read_text() == "keep me"


def test_an_empty_bundle_extracts_to_an_empty_folder(tmp_path):
    archive = write_bundle(tmp_path / "empty.tar.zst", {})
    archive.extract(tmp_path / "out")
    assert list((tmp_path / "out").iterdir()) == []


def test_a_missing_archive_is_an_extract_error(tmp_path):
    with pytest.raises(ExtractError) as raised:
        BundleArchive(tmp_path / "bundle.tar.zst").extract(tmp_path / "bundle")
    assert isinstance(raised.value.__cause__, FileNotFoundError)


def test_bytes_that_are_not_zstandard_are_an_extract_error(tmp_path):
    archive = BundleArchive(tmp_path / "bundle.tar.zst")
    archive.archive.write_bytes(b"this is not a zstd frame at all")
    with pytest.raises(ExtractError) as raised:
        archive.extract(tmp_path / "out")
    assert isinstance(raised.value.__cause__, zstandard.ZstdError)


def test_zstandard_around_a_broken_tar_is_an_extract_error(tmp_path):
    archive = BundleArchive(tmp_path / "bundle.tar.zst")
    archive.archive.write_bytes(
        zstandard.ZstdCompressor().compress(b"this is not a tar archive")
    )
    with pytest.raises(ExtractError) as raised:
        archive.extract(tmp_path / "out")
    assert isinstance(raised.value.__cause__, tarfile.ReadError)


def test_a_member_escaping_the_folder_is_refused(tmp_path):
    archive = write_bundle(tmp_path / "evil.tar.zst", {"../escape.txt": b"oops"})
    with pytest.raises(ExtractError) as raised:
        archive.extract(tmp_path / "out")
    assert isinstance(raised.value.__cause__, tarfile.FilterError)
    assert not (tmp_path / "escape.txt").exists()


def test_an_absolute_member_lands_inside_the_folder(tmp_path):
    archive = write_bundle(tmp_path / "abs.tar.zst", {"/etc/abs.txt": b"oops"})
    archive.extract(tmp_path / "out")
    assert (tmp_path / "out" / "etc" / "abs.txt").read_bytes() == b"oops"

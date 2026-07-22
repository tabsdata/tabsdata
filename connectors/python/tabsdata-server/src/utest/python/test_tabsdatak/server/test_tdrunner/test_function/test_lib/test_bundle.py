#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest
import zstandard

from tabsdatak.server._tdrunner.function._lib.bundle import extract_bundle

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _write_tar_zst(path: Path, files: dict[str, bytes]) -> None:
    """Write a `.tar.zst` (zstandard-compressed tar) containing `files`.

    Matches the shape of the producer at ``tabsdatak.common._archive.bundle.build_bundle``:
    raw file → zstd stream_writer → streaming tarfile.
    """
    compressor = zstandard.ZstdCompressor()
    with open(path, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, content in files.items():
                    info = tarfile.TarInfo(name=name)
                    info.size = len(content)
                    tar.addfile(info, io.BytesIO(content))


# --- happy path ---


def test_extract_writes_files(tmp_path):
    archive = tmp_path / "bundle.tar.zst"
    _write_tar_zst(archive, {"a.txt": b"hello", "subdir/b.txt": b"world"})
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    assert (dest / "a.txt").read_bytes() == b"hello"
    assert (dest / "subdir" / "b.txt").read_bytes() == b"world"


def test_extract_creates_missing_dest_parents(tmp_path):
    archive = tmp_path / "bundle.tar.zst"
    _write_tar_zst(archive, {"a.txt": b"hi"})
    dest = tmp_path / "deep" / "nested" / "out"
    extract_bundle(archive, dest)
    assert (dest / "a.txt").read_bytes() == b"hi"


def test_extract_into_existing_dest_preserves_content(tmp_path):
    archive = tmp_path / "bundle.tar.zst"
    _write_tar_zst(archive, {"new.txt": b"new"})
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "preexisting.txt").write_text("keep me")
    extract_bundle(archive, dest)
    assert (dest / "new.txt").read_bytes() == b"new"
    assert (dest / "preexisting.txt").read_text() == "keep me"


def test_extract_empty_archive(tmp_path):
    archive = tmp_path / "empty.tar.zst"
    _write_tar_zst(archive, {})
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    assert dest.is_dir()
    assert list(dest.iterdir()) == []


# --- error paths ---


def test_missing_archive_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract_bundle(tmp_path / "nope.tar.zst", tmp_path / "out")


def test_corrupt_zstd_bytes_raise_zstd_error(tmp_path):
    """Bytes that aren't valid zstd frames fail at decompression."""
    archive = tmp_path / "bad.tar.zst"
    archive.write_bytes(b"this is not a zstd frame at all")
    with pytest.raises(zstandard.ZstdError):
        extract_bundle(archive, tmp_path / "out")


def test_valid_zstd_but_corrupt_tar_raises_readerror(tmp_path):
    """A well-formed zstd frame wrapping garbage tar bytes makes it
    through decompression but trips tarfile's ReadError. This is the
    failure mode the old gzip path produced for any corrupt input."""
    archive = tmp_path / "bad-inner.tar.zst"
    compressed = zstandard.ZstdCompressor().compress(b"this is not a tar archive")
    archive.write_bytes(compressed)
    with pytest.raises(tarfile.ReadError):
        extract_bundle(archive, tmp_path / "out")


# --- safety: data-filter rejections ---


def test_path_traversal_via_relative_dotdot_is_rejected(tmp_path):
    archive = tmp_path / "evil.tar.zst"
    compressor = zstandard.ZstdCompressor()
    with open(archive, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                info = tarfile.TarInfo(name="../escape.txt")
                info.size = 4
                tar.addfile(info, io.BytesIO(b"oops"))
    with pytest.raises(tarfile.FilterError):
        extract_bundle(archive, tmp_path / "out")
    assert not (tmp_path / "escape.txt").exists()


def test_absolute_path_member_is_normalised_inside_dest(tmp_path):
    """The data filter strips leading slashes so abs paths land inside dest."""
    archive = tmp_path / "abs.tar.zst"
    compressor = zstandard.ZstdCompressor()
    with open(archive, "wb") as raw:
        with compressor.stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                info = tarfile.TarInfo(name="/etc/abs.txt")
                info.size = 4
                tar.addfile(info, io.BytesIO(b"oops"))
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    # Leading `/` stripped → file lives under dest, NOT at the absolute path.
    assert (dest / "etc" / "abs.txt").read_bytes() == b"oops"

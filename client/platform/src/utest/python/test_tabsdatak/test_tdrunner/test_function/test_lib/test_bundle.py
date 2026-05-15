#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from tabsdatak._tdrunner.function._lib.bundle import extract_bundle

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _write_targz(path: Path, files: dict[str, bytes]) -> None:
    with tarfile.open(path, "w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))


# --- happy path ---


def test_extract_writes_files(tmp_path):
    archive = tmp_path / "bundle.tar.gz"
    _write_targz(archive, {"a.txt": b"hello", "subdir/b.txt": b"world"})
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    assert (dest / "a.txt").read_bytes() == b"hello"
    assert (dest / "subdir" / "b.txt").read_bytes() == b"world"


def test_extract_creates_missing_dest_parents(tmp_path):
    archive = tmp_path / "bundle.tar.gz"
    _write_targz(archive, {"a.txt": b"hi"})
    dest = tmp_path / "deep" / "nested" / "out"
    extract_bundle(archive, dest)
    assert (dest / "a.txt").read_bytes() == b"hi"


def test_extract_into_existing_dest_preserves_content(tmp_path):
    archive = tmp_path / "bundle.tar.gz"
    _write_targz(archive, {"new.txt": b"new"})
    dest = tmp_path / "out"
    dest.mkdir()
    (dest / "preexisting.txt").write_text("keep me")
    extract_bundle(archive, dest)
    assert (dest / "new.txt").read_bytes() == b"new"
    assert (dest / "preexisting.txt").read_text() == "keep me"


def test_extract_empty_archive(tmp_path):
    archive = tmp_path / "empty.tar.gz"
    _write_targz(archive, {})
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    assert dest.is_dir()
    assert list(dest.iterdir()) == []


# --- error paths ---


def test_missing_archive_raises_filenotfound(tmp_path):
    with pytest.raises(FileNotFoundError):
        extract_bundle(tmp_path / "nope.tar.gz", tmp_path / "out")


def test_corrupt_archive_raises_readerror(tmp_path):
    archive = tmp_path / "bad.tar.gz"
    archive.write_bytes(b"this is not a tar archive at all")
    with pytest.raises(tarfile.ReadError):
        extract_bundle(archive, tmp_path / "out")


# --- safety: data-filter rejections ---


def test_path_traversal_via_relative_dotdot_is_rejected(tmp_path):
    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo(name="../escape.txt")
        info.size = 4
        tar.addfile(info, io.BytesIO(b"oops"))
    with pytest.raises(tarfile.FilterError):
        extract_bundle(archive, tmp_path / "out")
    assert not (tmp_path / "escape.txt").exists()


def test_absolute_path_member_is_normalised_inside_dest(tmp_path):
    """The data filter strips leading slashes so abs paths land inside dest."""
    archive = tmp_path / "abs.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo(name="/etc/abs.txt")
        info.size = 4
        tar.addfile(info, io.BytesIO(b"oops"))
    dest = tmp_path / "out"
    extract_bundle(archive, dest)
    # Leading `/` stripped → file lives under dest, NOT at the absolute path.
    assert (dest / "etc" / "abs.txt").read_bytes() == b"oops"

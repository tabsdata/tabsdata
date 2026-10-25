#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
from pathlib import Path

import pytest
from obstore.store import S3Store

from tabsdatak.common._storage import transfer
from tabsdatak.common._storage.transfer import StorageLocation, TransferError

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

KIBIBYTE = 1024
SIZE = 5 * transfer.MEBIBYTE + 17


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "source.bin"
    path.write_bytes(os.urandom(SIZE))
    return path


@pytest.fixture
def ranged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transfer, "STREAM_THRESHOLD", transfer.MEBIBYTE)
    monkeypatch.setattr(transfer, "RANGE_SIZE", 256 * KIBIBYTE)


def stored(tmp_path: Path) -> StorageLocation:
    return StorageLocation(uri=(tmp_path / "store" / "deep" / "object.bin").as_uri())


def test_upload_creates_the_target_folders(tmp_path: Path, source: Path):
    assert transfer.upload(source, stored(tmp_path)) == SIZE
    assert (tmp_path / "store" / "deep" / "object.bin").read_bytes() == (
        source.read_bytes()
    )


def test_streamed_download_round_trips(tmp_path: Path, source: Path):
    transfer.upload(source, stored(tmp_path))
    target = tmp_path / "out" / "object.bin"
    assert transfer.download(stored(tmp_path), target) == SIZE
    assert target.read_bytes() == source.read_bytes()


def test_ranged_download_round_trips(tmp_path: Path, source: Path, ranged: None):
    transfer.upload(source, stored(tmp_path))
    target = tmp_path / "out" / "object.bin"
    assert transfer.download(stored(tmp_path), target) == SIZE
    assert target.read_bytes() == source.read_bytes()


def test_ranged_chunks_keep_their_order(tmp_path: Path, source: Path, ranged: None):
    transfer.upload(source, stored(tmp_path))
    assert b"".join(transfer.chunks(stored(tmp_path))) == source.read_bytes()


def test_failed_download_leaves_no_files(tmp_path: Path):
    missing = StorageLocation(uri=(tmp_path / "store" / "missing.bin").as_uri())
    target = tmp_path / "out" / "missing.bin"
    with pytest.raises(FileNotFoundError):
        transfer.download(missing, target)
    assert list(target.parent.iterdir()) == []


def test_location_must_name_an_object():
    with pytest.raises(TransferError, match="does not name an object"):
        transfer.split("s3://bucket/")


def test_stores_are_reused_per_root_and_options(tmp_path: Path):
    root = tmp_path.as_uri()
    assert transfer.store(root, {}) is transfer.store(root, {})


def test_ambient_credentials_are_ignored(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "ambient")
    explicit = {"aws_region": "eu-west-1", "aws_skip_signature": "true"}
    with pytest.raises(Exception, match="SecretAccessKey"):
        S3Store("ambient-bucket", config=explicit)
    transfer.build("s3://ambient-bucket", explicit)
    assert os.environ["AWS_ACCESS_KEY_ID"] == "ambient"


def test_unsupported_scheme_fails():
    with pytest.raises(TransferError, match="unsupported scheme"):
        transfer.build("ftp://host/folder", {})

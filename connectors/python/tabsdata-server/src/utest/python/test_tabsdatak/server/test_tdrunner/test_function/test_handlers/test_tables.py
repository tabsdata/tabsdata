#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    EnvironmentMountDeclaration,
    MountError,
)
from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FRAME = pl.DataFrame({"id": [1, 2]})


def written(tmp_path):
    path = tmp_path / "table.parquet"
    FRAME.write_parquet(path)
    return path


def test_a_record_with_a_location_reads_from_its_mount():
    table = TableFile.of(
        InputRecord(location=Location(uri="s3://b/t.parquet", env_prefix="ROOT"))
    )
    assert table.uri == "s3://b/t.parquet"
    assert table.declaration == EnvironmentMountDeclaration(id="ROOT")


def test_a_record_with_a_location_without_mount_reads_anonymously():
    table = TableFile.of(InputRecord(location=Location(uri="file:///t.parquet")))
    assert table.declaration == AnonymousMountDeclaration()


def test_a_record_with_a_file_reads_it_locally():
    table = TableFile.of(InputRecord(file="/t.parquet"))
    assert table.uri == "file:///t.parquet"
    assert table.declaration == AnonymousMountDeclaration()


def test_the_location_wins_over_the_file():
    table = TableFile.of(
        InputRecord(file="/local.parquet", location=Location(uri="file:///t.parquet"))
    )
    assert table.uri == "file:///t.parquet"


def test_a_record_without_data_has_no_table_file():
    assert TableFile.of(InputRecord()) is None


def test_a_local_table_file_is_an_absolute_file_uri(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert TableFile.local(Path("t.parquet")).uri == (Path.cwd() / "t.parquet").as_uri()


def test_scan_a_stored_table(tmp_path):
    path = written(tmp_path)
    table = TableFile.stored(Location(uri=path.as_uri(), env_prefix="ROOT"))
    assert table.scan().collect().equals(FRAME)


def test_scan_a_local_file(tmp_path):
    table = TableFile.of(InputRecord(file=str(written(tmp_path))))
    assert table.scan().collect().equals(FRAME)


def test_scan_reads_the_schema_at_once(tmp_path):
    path = tmp_path / "table.parquet"
    path.write_bytes(b"not a parquet")
    with pytest.raises(Exception):
        TableFile.local(path).scan()


def test_scan_rejects_a_mount_without_its_options(monkeypatch):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__REGION", "eu")
    table = TableFile.stored(Location(uri="s3://b/t.parquet", env_prefix="ROOT"))
    with pytest.raises(MountError):
        table.scan()

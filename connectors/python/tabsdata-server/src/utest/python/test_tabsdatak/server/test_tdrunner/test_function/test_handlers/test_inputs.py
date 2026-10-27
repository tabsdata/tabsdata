#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    EnvironmentMountDeclaration,
    MountError,
)
from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._handlers.inputs import InputSource

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FRAME = pl.DataFrame({"id": [1, 2]})


def written(tmp_path):
    path = tmp_path / "table.parquet"
    FRAME.write_parquet(path)
    return path


def test_a_record_with_a_location_reads_from_its_mount():
    source = InputSource.of(
        InputRecord(location=Location(uri="s3://b/t.parquet", env_prefix="ROOT"))
    )
    assert source.uri == "s3://b/t.parquet"
    assert source.declaration == EnvironmentMountDeclaration(id="ROOT")


def test_a_record_with_a_location_without_mount_reads_anonymously():
    source = InputSource.of(InputRecord(location=Location(uri="file:///t.parquet")))
    assert source.declaration == AnonymousMountDeclaration()


def test_a_record_with_a_file_reads_it_locally():
    source = InputSource.of(InputRecord(file="/t.parquet"))
    assert source.uri == "/t.parquet"
    assert source.declaration == AnonymousMountDeclaration()


def test_the_location_wins_over_the_file():
    source = InputSource.of(
        InputRecord(file="/local.parquet", location=Location(uri="file:///t.parquet"))
    )
    assert source.uri == "file:///t.parquet"


def test_a_record_without_data_has_no_source():
    assert InputSource.of(InputRecord()) is None


def test_scan_a_stored_table(tmp_path):
    path = written(tmp_path)
    source = InputSource.of(
        InputRecord(location=Location(uri=path.as_uri(), env_prefix="ROOT"))
    )
    assert source.scan().collect().equals(FRAME)


def test_scan_a_local_file(tmp_path):
    source = InputSource.of(InputRecord(file=str(written(tmp_path))))
    assert source.scan().collect().equals(FRAME)


def test_scan_reads_the_schema_at_once(tmp_path):
    path = tmp_path / "table.parquet"
    path.write_bytes(b"not a parquet")
    with pytest.raises(Exception):
        InputSource.of(InputRecord(file=str(path))).scan()


def test_scan_rejects_a_mount_without_its_options(monkeypatch):
    monkeypatch.setenv("SECRETS__STORAGE__MOUNTS__ROOT__OPTIONS__REGION", "eu")
    source = InputSource.of(
        InputRecord(location=Location(uri="s3://b/t.parquet", env_prefix="ROOT"))
    )
    with pytest.raises(MountError):
        source.scan()

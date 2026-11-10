#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    EnvironmentMountDeclaration,
    MountError,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._commons.manifests.output import TableStats
from tabsdatak.server._tdrunner.function._handlers.tables import (
    TableFile,
    TableFileError,
)

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
    assert TableFile.local(written(tmp_path)).scan().collect().equals(FRAME)


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


def test_read_a_stored_table(tmp_path):
    path = written(tmp_path)
    table = TableFile.stored(Location(uri=path.as_uri(), env_prefix="ROOT"))
    assert table.read().equals(FRAME)


def test_read_a_missing_file_fails(tmp_path):
    with pytest.raises(Exception):
        TableFile.local(tmp_path / "missing.parquet").read()


def test_copy_is_lazy_until_collected(tmp_path):
    target = tmp_path / "data" / "v1" / "0.t"
    plan = TableFile.local(written(tmp_path)).copy(
        TableFile.stored(Location(uri=target.as_uri(), env_prefix="ROOT"))
    )
    assert not target.exists()
    pl.collect_all([plan], engine="streaming")
    assert pl.read_parquet(target).equals(FRAME)


def test_copy_keeps_a_file_without_columns(tmp_path):
    source = tmp_path / "empty.parquet"
    pl.DataFrame().write_parquet(source)
    target = tmp_path / "copy.parquet"
    pl.collect_all(
        [TableFile.local(source).copy(TableFile.local(target))], engine="streaming"
    )
    assert pl.read_parquet(target).shape == (0, 0)


def test_locator_is_the_table_uri(tmp_path):
    path = tmp_path / "t.parquet"
    table = TableFile.stored(Location(uri=path.as_uri(), env_prefix="ROOT"))
    assert table.locator().uri == path.as_uri()


def test_stats_match_the_whole_file(tmp_path):
    path = written(tmp_path)
    schema = pq.read_schema(path)
    assert TableFile.local(path).stats() == TableStats(
        column_count=len(schema),
        row_count=FRAME.height,
        schema_hash="sha256:"
        + hashlib.sha256(schema.serialize().to_pybytes()).hexdigest(),
        data_bytes=path.stat().st_size,
    )


def test_stats_of_a_stored_table(tmp_path):
    path = written(tmp_path)
    table = TableFile.stored(Location(uri=path.as_uri(), env_prefix="ROOT"))
    assert table.stats() == TableFile.local(path).stats()


def test_stats_of_an_empty_table(tmp_path):
    path = tmp_path / "empty.parquet"
    FRAME.clear().write_parquet(path)
    stats = TableFile.local(path).stats()
    assert (stats.column_count, stats.row_count) == (1, 0)


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b"PAR1",
        b"PAR1" + b"x" * 64,
        b"PAR1" + b"x" * 64 + b"xxxxPAR1",
    ],
    ids=["empty", "magic only", "no trailer", "truncated footer"],
)
def test_stats_reject_a_file_that_is_not_a_parquet(tmp_path, content):
    path = tmp_path / "t.parquet"
    path.write_bytes(content)
    with pytest.raises(TableFileError):
        TableFile.local(path).stats()


def test_stats_reject_a_footer_that_is_not_parquet_metadata(tmp_path):
    path = tmp_path / "t.parquet"
    path.write_bytes(b"PAR1" + b"x" * 64 + (16).to_bytes(4, "little") + b"PAR1")
    with pytest.raises(TableFileError, match="unreadable footer"):
        TableFile.local(path).stats()


def test_stats_of_a_missing_file_fail(tmp_path):
    with pytest.raises(Exception):
        TableFile.local(tmp_path / "absent.parquet").stats()


EXTENSION = pl.Extension("test.ext", pl.Int64)


def test_sink_stores_extension_types_as_their_storage(tmp_path):
    frame = pl.DataFrame({
        "plain": [1, None],
        "ext": pl.Series([1, None]).cast(EXTENSION),
        "listed": pl.Series([[1], None]).cast(pl.List(EXTENSION)),
        "arrayed": pl.Series([[1, 2], None]).cast(pl.Array(EXTENSION, 2)),
        "nested": pl.Series([{"a": 1, "b": "x"}, None]).cast(
            pl.Struct({"a": EXTENSION, "b": pl.String})
        ),
    })
    target = TableFile.local(tmp_path / "out.parquet")
    target.sink(frame.lazy()).collect()
    stored = pl.read_parquet(tmp_path / "out.parquet")
    assert stored.schema == pl.Schema({
        "plain": pl.Int64,
        "ext": pl.Int64,
        "listed": pl.List(pl.Int64),
        "arrayed": pl.Array(pl.Int64, 2),
        "nested": pl.Struct({"a": pl.Int64, "b": pl.String}),
    })
    assert stored.to_dicts() == [
        {"plain": 1, "ext": 1, "listed": [1], "arrayed": [1, 2],
         "nested": {"a": 1, "b": "x"}},
        {"plain": None, "ext": None, "listed": None, "arrayed": None,
         "nested": None},
    ]


def test_sink_refuses_an_extension_type_inside_a_map(tmp_path):
    frame = pl.LazyFrame(
        {"m": pl.Series([{"k": 1}], dtype=pl.Map(pl.String, pl.Int64))}
    ).with_columns(pl.col("m").cast(pl.Map(pl.String, EXTENSION)))
    with pytest.raises(TableFileError, match="'m' cannot be stored"):
        TableFile.local(tmp_path / "out.parquet").sink(frame)

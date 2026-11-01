#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from tabsdatak.common._storage.landed import (
    TABSDATA_LANDING_FACTS_KEY,
    LandingFacts,
    SchemaOrigin,
)
from tabsdatak.common._storage.transfer import StorageLocation
from tabsdatak.conn.common._cursor import LastModifiedCursor
from tabsdatak.conn.stream.stage import (
    MANIFEST_CURSOR_KEY,
    MANIFEST_INITIAL_LASTMOD,
    ManifestStageSrcPlugin,
)
from tabsdatak.conn.stream.stage.error import ManifestStageException
from tabsdatak.spi import StreamStageSrc, StreamStageSrcPlugin

NOON = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc).timestamp()


class Ctx:
    def __init__(self, work_dir: Path, attrs: dict | None = None) -> None:
        self.work_dir = work_dir
        self.attrs: dict = dict(attrs or {})

    def get_attr(self, attr):
        return self.attrs.get(attr)

    def set_attr(self, attr, value):
        self.attrs[attr] = value


class Connection:
    def __init__(self, stage: Path) -> None:
        self.location = StorageLocation(stage.as_uri())


@pytest.fixture
def stage(tmp_path: Path) -> Path:
    folder = tmp_path / "stage"
    (folder / "manifests").mkdir(parents=True)
    return folder


def manifest(stage: Path, batch: str, tables: list[list[str]], at: float) -> None:
    path = stage / "manifests" / f"stream-manifest-{batch}.json"
    path.write_text(json.dumps(tables))
    os.utime(path, (at, at))


def parquet(stage: Path, name: str, ids: list[int]) -> str:
    path = stage / "files" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": ids}).write_parquet(path)
    return f"files/{name}"


def landed(
    stage: Path,
    stem: str,
    frame: pl.DataFrame,
    *,
    schema: pl.Schema | None = None,
    metadata: dict | None = None,
) -> str:
    folder = stage / "files" / "b1"
    folder.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(folder / f"{stem}.t")
    if schema is None:
        origin, target = SchemaOrigin.DECLARED, frame.schema
    else:
        origin, target = SchemaOrigin.INFERRED, schema
    facts = LandingFacts(rows=frame.height, origin=origin, metadata=metadata)
    pl.DataFrame(schema=target).write_parquet(
        folder / f"{stem}.s",
        metadata={TABSDATA_LANDING_FACTS_KEY: facts.model_dump_json()},
    )
    return f"files/b1/{stem}.t"


def read(stage: Path, ctx: Ctx):
    return ManifestStageSrcPlugin().read_in(ctx, Connection(stage), StreamStageSrc())


def ids(tables) -> list[list[list[int]]]:
    return [
        [table.frame.collect()["id"].to_list() for table in slot] for slot in tables
    ]


def test_without_manifests_nothing_is_read(stage, tmp_path):
    ctx = Ctx(tmp_path)
    assert read(stage, ctx) == []
    assert MANIFEST_CURSOR_KEY in ctx.attrs


def test_manifests_are_read_one_per_run_oldest_first(stage, tmp_path):
    manifest(stage, "b", [[parquet(stage, "b.parquet", [2])]], NOON + 10)
    manifest(stage, "a", [[parquet(stage, "a.parquet", [1])]], NOON)
    ctx = Ctx(tmp_path)
    assert ids(read(stage, ctx)) == [[[1]]]
    assert ids(read(stage, ctx)) == [[[2]]]
    assert read(stage, ctx) == []


def test_manifests_sharing_a_time_are_each_read_once(stage, tmp_path):
    manifest(stage, "a", [[parquet(stage, "a.parquet", [1])]], NOON)
    manifest(stage, "b", [[parquet(stage, "b.parquet", [2])]], NOON)
    ctx = Ctx(tmp_path)
    assert ids(read(stage, ctx)) == [[[1]]]
    assert ids(read(stage, ctx)) == [[[2]]]
    assert read(stage, ctx) == []


def test_a_stored_cursor_resumes_past_what_it_took(stage, tmp_path):
    manifest(stage, "a", [[parquet(stage, "a.parquet", [1])]], NOON)
    manifest(stage, "b", [[parquet(stage, "b.parquet", [2])]], NOON + 10)
    cursor = LastModifiedCursor.decode(None, MANIFEST_INITIAL_LASTMOD)
    pattern = f"{stage.as_posix()}/manifests/stream-manifest-*.json"
    cursor.take(
        pattern,
        "stream-manifest-a.json",
        datetime.fromtimestamp(NOON, tz=timezone.utc),
    )
    ctx = Ctx(tmp_path, {MANIFEST_CURSOR_KEY: cursor.encode()})
    assert ids(read(stage, ctx)) == [[[2]]]


def test_every_slot_reads_its_tables_in_order(stage, tmp_path):
    first = parquet(stage, "a.parquet", [1])
    second = parquet(stage, "b.parquet", [2])
    third = parquet(stage, "c.parquet", [3])
    manifest(stage, "a", [[first, second], [], [third]], NOON)
    assert ids(read(stage, Ctx(tmp_path))) == [[[1], [2]], [], [[3]]]


def test_a_landed_table_is_read_with_its_metadata(stage, tmp_path):
    name = landed(stage, "0.0", pl.DataFrame({"id": [7]}), metadata={"@src": "k"})
    manifest(stage, "a", [[name]], NOON)
    ((table,),) = read(stage, Ctx(tmp_path))
    assert table.frame.collect()["id"].to_list() == [7]
    assert table.schema is None
    assert table.metadata == {"@src": "k"}


def test_an_untyped_landed_table_carries_its_inferred_schema(stage, tmp_path):
    inferred = pl.Schema({"id": pl.Int64})
    name = landed(stage, "0.0", pl.DataFrame({"id": ["7"]}), schema=inferred)
    manifest(stage, "a", [[name]], NOON)
    ((table,),) = read(stage, Ctx(tmp_path))
    assert table.frame.collect()["id"].to_list() == ["7"]
    assert table.schema == inferred


def test_files_and_landed_tables_mix_in_one_manifest(stage, tmp_path):
    old = parquet(stage, "a.parquet", [1])
    new = landed(stage, "0.0", pl.DataFrame({"id": [2]}))
    manifest(stage, "a", [[old, new]], NOON)
    assert ids(read(stage, Ctx(tmp_path))) == [[[1], [2]]]


@pytest.mark.parametrize(
    "content, code",
    [("not json", "MANIFEST_STAGE_6"), (json.dumps({"a": 1}), "MANIFEST_STAGE_7")],
    ids=["not json", "not lists"],
)
def test_a_manifest_that_cannot_be_read_is_refused(stage, tmp_path, content, code):
    path = stage / "manifests" / "stream-manifest-a.json"
    path.write_text(content)
    with pytest.raises(ManifestStageException, match=code):
        read(stage, Ctx(tmp_path))


def test_the_stream_stage_plugin_reads_through_the_manifest_plugin(stage, tmp_path):
    manifest(stage, "a", [[parquet(stage, "a.parquet", [1])]], NOON)
    tables = StreamStageSrcPlugin().read_in(
        Ctx(tmp_path), Connection(stage), StreamStageSrc()
    )
    assert ids(tables) == [[[1]]]

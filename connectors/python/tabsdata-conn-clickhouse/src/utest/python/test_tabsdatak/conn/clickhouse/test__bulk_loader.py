#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for ``_bulk_loader.py`` phase orchestration -- focused on the
``if_table_exists='replace'`` contract: the target is dropped *before* the
create/verify loop, so a re-run recreates it fresh (and a same-name re-run can
no longer collide). ``append`` must never drop.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._bulk_loader import CloudStagedBulkLoader

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._cloud_storage import CloudStorage, StagedFile

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import SchemaEvolutionMode, WriteMode
from tabsdatak.conn.common.file._store import FileStore

pytestmark = pytest.mark.clickhouse


class _RecordingDialect:
    """Records the order of the schema/DDL calls the loader makes."""

    def __init__(self, exists: bool):
        self._exists = exists
        self.calls: list[str] = []
        self.schemas: list = []
        self.nulls: list = []

    def drop_tables(self, load_mode, table_names):
        self.calls.append("drop_tables")
        self._exists = False

    def table_exists(self, load_mode, table_name):
        self.calls.append("table_exists")
        return self._exists

    def create_table(self, load_mode, table_name, arrow_schema, column_nulls=None):
        self.calls.append("create_table")
        self.schemas.append(arrow_schema)
        self.nulls.append(column_nulls)

    def supports_auto_cleanup(self):
        return False

    def supports_transactions(self):
        return False

    def bulk_load_tables(self, load_mode, items):
        raise RuntimeError("load refused")


class _LocalStorage(CloudStorage):
    def __init__(self, root: Path):
        self._root = root
        self.deleted: list[str] = []

    @property
    def base_path(self) -> str:
        return "stage"

    def store(self) -> FileStore:
        return FileStore(self._root.as_uri())

    def staged_ref(self, key: str) -> str:
        return f"ref:{key}"

    def delete(self, up_to_id: str) -> None:
        self.deleted.append(up_to_id)
        raise OSError("storage unavailable")


ROWS = pl.LazyFrame({"id": [1, 2], "name": ["x", None]})


def _loader(
    tmp_path: Path,
    dialect,
    write_mode: WriteMode,
    evolution: SchemaEvolutionMode = SchemaEvolutionMode.NONE,
) -> CloudStagedBulkLoader:
    return CloudStagedBulkLoader(
        cloud_storage=_LocalStorage(tmp_path),
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=evolution,
        upload_id="u1",
    )


def _staged(loader: CloudStagedBulkLoader) -> dict[str, list[StagedFile]]:
    return loader.phase_upload({"users": [("0.parquet", ROWS)]})


def test_frames_are_staged_under_the_upload_id(tmp_path):
    loader = _loader(tmp_path, _RecordingDialect(exists=False), WriteMode.APPEND)
    assert _staged(loader) == {
        "users": [StagedFile("ref:stage/u1/0.parquet", "stage/u1/0.parquet")]
    }
    assert pl.read_parquet(tmp_path / "stage" / "u1" / "0.parquet").equals(
        ROWS.collect()
    )


def test_the_table_is_created_from_the_staged_footer(tmp_path):
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, SchemaEvolutionMode.NATIVE)
    loader.phase_validate_and_prepare(_staged(loader))
    [schema] = dialect.schemas
    assert schema.names == ["id", "name"]
    [nulls] = dialect.nulls
    assert nulls == {"id": False, "name": True}


def test_replace_drops_before_create(tmp_path):
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.calls[0] == "drop_tables"
    assert dialect.calls.index("drop_tables") < dialect.calls.index("create_table")


def test_replace_drops_even_when_table_exists(tmp_path):
    # A re-run against a pre-existing table: the drop must still fire (so the
    # table is recreated fresh) rather than being skipped.
    dialect = _RecordingDialect(exists=True)
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.calls[0] == "drop_tables"


def test_append_does_not_drop(tmp_path):
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.APPEND)
    loader.phase_validate_and_prepare(_staged(loader))
    assert "drop_tables" not in dialect.calls


def test_a_failed_load_still_cleans_up_and_a_failed_cleanup_is_ignored(tmp_path):
    loader = _loader(tmp_path, _RecordingDialect(exists=False), WriteMode.APPEND)
    with pytest.raises(RuntimeError, match="load refused"):
        loader.load({"users": [("0.parquet", ROWS)]})
    assert loader.cloud_storage.deleted == ["u1"]

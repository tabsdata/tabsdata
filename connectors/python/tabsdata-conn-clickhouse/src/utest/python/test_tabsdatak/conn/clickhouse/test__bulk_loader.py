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
from tabsdatak.conn.clickhouse._modes import WriteMode
from tabsdatak.conn.common.file._store import FileStore

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.delta import Action, SchemaChange, SchemaDelta

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.policy import STRICT, UPDATE
from tabsdatak.conn.common.sql.error import SqlCommonRuntimeException

pytestmark = pytest.mark.clickhouse


class _RecordingDialect:
    """Records the order of the schema/DDL calls the loader makes."""

    def __init__(self, exists: bool):
        self._exists = exists
        self.calls: list[str] = []
        self.schemas: list = []

    def drop_tables(self, load_mode, table_names):
        self.calls.append("drop_tables")
        self._exists = False

    def table_exists(self, load_mode, table_name):
        self.calls.append("table_exists")
        return self._exists

    def create_table(self, load_mode, table_name, arrow_schema):
        self.calls.append("create_table")
        self.schemas.append(arrow_schema)

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
    evolution: str = STRICT,
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


@pytest.mark.parametrize("evolution", [STRICT, UPDATE])
def test_the_table_is_created_nullable_from_the_staged_footer(tmp_path, evolution):
    # Every column nullable whatever the first batch held, so a later batch
    # that lacks one leaves NULL there rather than ClickHouse's default.
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, evolution)
    loader.phase_validate_and_prepare(_staged(loader))
    [schema] = dialect.schemas
    assert schema.names == ["id", "name"]
    assert all(field.nullable for field in schema)


class _Evolving(_RecordingDialect):
    def __init__(self, changes: list[SchemaChange]):
        super().__init__(exists=True)
        self._changes = changes
        self.applied: list[SchemaChange] = []

    def compute_evolution_delta(self, load_mode, table_name, arrow_schema, nulls):
        return SchemaDelta(table_name=table_name, is_create=False, changes=self._changes)

    def apply_schema_delta(self, load_mode, table_name, delta):
        self.applied.extend(delta.changes)


_DROPPED = SchemaChange(action=Action.DROP_COLUMN, column="score", safe=True)
_ADDED = SchemaChange(
    action=Action.ADD_COLUMN, column="extra", safe=True, detail={"type": "String"}
)


@pytest.mark.parametrize("evolution", [STRICT, UPDATE])
def test_a_column_the_batch_lacks_is_never_dropped(tmp_path, evolution):
    dialect = _Evolving([_DROPPED])
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, evolution)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.applied == []


def test_strict_refuses_a_new_column(tmp_path):
    loader = _loader(tmp_path, _Evolving([_ADDED]), WriteMode.APPEND, STRICT)
    with pytest.raises(SqlCommonRuntimeException):
        loader.phase_validate_and_prepare(_staged(loader))


def _widened(old: str = "Int32") -> SchemaChange:
    return SchemaChange(
        action=Action.ALTER_TYPE,
        column="id",
        safe=True,
        detail={"old_type": old, "new_type": "Int64", "nullable": True},
    )


def _staged_ids(loader: CloudStagedBulkLoader, ids: list) -> dict:
    frame = pl.LazyFrame({"id": ids}, schema={"id": pl.Int64})
    return loader.phase_upload({"users": [("0.parquet", frame)]})


@pytest.mark.parametrize(
    "ids", [[1, 2], [-(2**31), 2**31 - 1], [None, None], [None, 7]]
)
def test_strict_takes_integers_their_narrower_column_holds(tmp_path, ids):
    """Read off the staged footer's statistics, with no pass over the data."""
    dialect = _Evolving([_widened()])
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, STRICT)
    loader.phase_validate_and_prepare(_staged_ids(loader, ids))
    assert dialect.applied == []


@pytest.mark.parametrize(
    "old,ids", [("Int32", [1, 2**40]), ("Int32", [-(2**31) - 1]), ("UInt32", [-1, 5])]
)
def test_strict_refuses_integers_their_narrower_column_cannot_hold(
    tmp_path, old, ids
):
    """ClickHouse would cut them to the column's width."""
    loader = _loader(tmp_path, _Evolving([_widened(old)]), WriteMode.APPEND, STRICT)
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        loader.phase_validate_and_prepare(_staged_ids(loader, ids))


def test_update_widens_a_column_whatever_its_values(tmp_path):
    dialect = _Evolving([_widened()])
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, UPDATE)
    loader.phase_validate_and_prepare(_staged_ids(loader, [1, 2]))
    assert dialect.applied == [_widened()]


def test_update_adds_a_new_column(tmp_path):
    dialect = _Evolving([_ADDED, _DROPPED])
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, UPDATE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.applied == [_ADDED]


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


def test_the_load_names_the_staged_columns(tmp_path):
    loaded = []

    class _Loading(_RecordingDialect):
        def bulk_load_tables(self, load_mode, items):
            loaded.extend(items)
            return []

        def is_bulk_load_async(self):
            return False

    loader = _loader(tmp_path, _Loading(exists=False), WriteMode.APPEND)
    loader.check_bulk_load_results = lambda items, results: None
    loader.load({"users": [("0.parquet", ROWS)]})
    assert loaded == [("users", ["ref:stage/u1/0.parquet"], ["id", "name"])]

#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for ``_bulk_loader.py`` phase orchestration -- focused on the
``if_table_exists='replace'`` contract: the target is kept, evolved like an
append, and emptied only once every table took the batch's schema. ``append``
never empties it.
"""

from __future__ import annotations

from pathlib import Path

from decimal import Decimal

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

    def truncate_tables(self, load_mode, table_names):
        self.calls.append("truncate_tables")

    def compute_evolution_delta(self, load_mode, table_name, arrow_schema, nulls):
        self.calls.append("compute_evolution_delta")
        return SchemaDelta(table_name=table_name, is_create=False, changes=[])

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
        return SchemaDelta(
            table_name=table_name, is_create=False, changes=self._changes
        )

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


def _decimal_change(stored: str, batch: str) -> SchemaChange:
    return SchemaChange(
        action=Action.ALTER_TYPE,
        column="d",
        safe=False,
        detail={"old_type": stored, "new_type": batch, "nullable": True},
    )


def _staged_decimals(loader: CloudStagedBulkLoader, values: list, scale: int) -> dict:
    frame = pl.LazyFrame(
        {"d": [None if v is None else Decimal(v) for v in values]},
        schema={"d": pl.Decimal(38, scale)},
    )
    return loader.phase_upload({"users": [("0.parquet", frame)]})


@pytest.mark.parametrize(
    "stored,batch,values,scale",
    [
        ("Decimal(10, 2)", "Decimal(38, 2)", ["-99999999.99", "99999999.99"], 2),
        ("Decimal(10, 2)", "Decimal(38, 1)", ["12.5"], 1),
        ("Decimal(10, 2)", "Decimal(38, 2)", [None], 2),
    ],
)
def test_strict_takes_decimals_their_narrower_column_holds(
    tmp_path, stored, batch, values, scale
):
    dialect = _Evolving([_decimal_change(stored, batch)])
    loader = _loader(tmp_path, dialect, WriteMode.APPEND, STRICT)
    loader.phase_validate_and_prepare(_staged_decimals(loader, values, scale))
    assert dialect.applied == []


@pytest.mark.parametrize(
    "stored,batch,values,scale",
    [
        ("Decimal(10, 2)", "Decimal(38, 2)", ["100000000.00"], 2),
        ("Decimal(10, 2)", "Decimal(38, 2)", ["-100000000.00"], 2),
        # More fractional digits than the column keeps would be rounded away.
        ("Decimal(10, 2)", "Decimal(38, 4)", ["1.2500"], 4),
    ],
)
def test_strict_refuses_decimals_their_narrower_column_cannot_hold(
    tmp_path, stored, batch, values, scale
):
    loader = _loader(
        tmp_path, _Evolving([_decimal_change(stored, batch)]), WriteMode.APPEND, STRICT
    )
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        loader.phase_validate_and_prepare(_staged_decimals(loader, values, scale))


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


def test_replace_creates_a_missing_table_then_truncates(tmp_path):
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.calls == ["table_exists", "create_table", "truncate_tables"]


def test_replace_evolves_an_existing_table_then_truncates_it(tmp_path):
    dialect = _RecordingDialect(exists=True)
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.calls == [
        "table_exists",
        "compute_evolution_delta",
        "truncate_tables",
    ]


def test_replace_under_strict_refuses_a_new_column_before_truncating(tmp_path):
    dialect = _Evolving([_ADDED])
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE, STRICT)
    with pytest.raises(SqlCommonRuntimeException):
        loader.phase_validate_and_prepare(_staged(loader))
    assert "truncate_tables" not in dialect.calls


def test_replace_under_update_adds_a_new_column_then_truncates(tmp_path):
    dialect = _Evolving([_ADDED])
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE, UPDATE)
    loader.phase_validate_and_prepare(_staged(loader))
    assert dialect.applied == [_ADDED]
    assert dialect.calls[-1] == "truncate_tables"


def test_replace_under_strict_takes_integers_their_column_holds_then_truncates(
    tmp_path,
):
    dialect = _Evolving([_widened()])
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE, STRICT)
    loader.phase_validate_and_prepare(_staged_ids(loader, [1, 2]))
    assert dialect.applied == []
    assert dialect.calls[-1] == "truncate_tables"


def test_replace_under_strict_refuses_an_overflow_before_truncating(tmp_path):
    dialect = _Evolving([_widened()])
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE, STRICT)
    with pytest.raises(SqlCommonRuntimeException, match="SQL_COMMON_10"):
        loader.phase_validate_and_prepare(_staged_ids(loader, [1, 2**40]))
    assert "truncate_tables" not in dialect.calls


def test_replace_under_update_widens_then_truncates(tmp_path):
    dialect = _Evolving([_widened()])
    loader = _loader(tmp_path, dialect, WriteMode.REPLACE, UPDATE)
    loader.phase_validate_and_prepare(_staged_ids(loader, [1, 2**40]))
    assert dialect.applied == [_widened()]
    assert dialect.calls[-1] == "truncate_tables"


def test_append_does_not_truncate(tmp_path):
    dialect = _RecordingDialect(exists=False)
    loader = _loader(tmp_path, dialect, WriteMode.APPEND)
    loader.phase_validate_and_prepare(_staged(loader))
    assert "truncate_tables" not in dialect.calls


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

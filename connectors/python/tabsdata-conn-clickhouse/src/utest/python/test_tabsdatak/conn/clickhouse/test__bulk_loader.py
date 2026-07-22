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
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._bulk_loader import CloudStagedBulkLoader

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse._modes import SchemaEvolutionMode, WriteMode

pytestmark = pytest.mark.clickhouse


class _RecordingDialect:
    """Records the order of the schema/DDL calls the loader makes."""

    def __init__(self, exists: bool):
        self._exists = exists
        self.calls: list[str] = []

    def drop_tables(self, load_mode, table_names):
        self.calls.append("drop_tables")
        self._exists = False  # the dropped table no longer exists

    def table_exists(self, load_mode, table_name):
        self.calls.append("table_exists")
        return self._exists

    def create_table(self, load_mode, table_name, arrow_schema, column_nulls=None):
        self.calls.append("create_table")


def _parquet(path: Path) -> Path:
    pq.write_table(pa.table({"id": [1], "name": ["x"]}), path)
    return path


def _loader(dialect, write_mode: WriteMode) -> CloudStagedBulkLoader:
    return CloudStagedBulkLoader(
        cloud_storage=MagicMock(),
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id="u1",
    )


def test_replace_drops_before_create(tmp_path):
    dialect = _RecordingDialect(exists=False)
    _loader(dialect, WriteMode.REPLACE).phase_validate_and_prepare(
        {"users": [_parquet(tmp_path / "u.parquet")]}
    )
    assert dialect.calls[0] == "drop_tables"
    assert dialect.calls.index("drop_tables") < dialect.calls.index("create_table")


def test_replace_drops_even_when_table_exists(tmp_path):
    # A re-run against a pre-existing table: the drop must still fire (so the
    # table is recreated fresh) rather than being skipped.
    dialect = _RecordingDialect(exists=True)
    _loader(dialect, WriteMode.REPLACE).phase_validate_and_prepare(
        {"users": [_parquet(tmp_path / "u.parquet")]}
    )
    assert "drop_tables" in dialect.calls
    assert dialect.calls[0] == "drop_tables"


def test_append_does_not_drop(tmp_path):
    dialect = _RecordingDialect(exists=False)
    _loader(dialect, WriteMode.APPEND).phase_validate_and_prepare(
        {"users": [_parquet(tmp_path / "u.parquet")]}
    )
    assert "drop_tables" not in dialect.calls

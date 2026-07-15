#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for :class:`SqlAlchemyConnectionExplorer.metadata`.

Driven over an on-disk SQLite database so the real reflection path
(``inspect`` -> ``get_table_names`` -> ``get_columns``) runs without a
live server. The concurrent-DDL race is simulated by wrapping the
inspector so one listed table raises ``NoSuchTableError`` when reflected,
as if it were dropped between the table listing and the per-table
reflection.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.exc import NoSuchTableError

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql import _explorer as explorer_mod

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._explorer import SqlAlchemyConnectionExplorer


class _SqliteExplorer(SqlAlchemyConnectionExplorer):
    """Explorer bound to a SQLite URI; ``make_url`` ignores the conn."""

    def __init__(self, uri: str):
        self._uri = uri

    def make_url(self, conn):
        return self._uri

    def schema_filter(self, conn) -> str:
        return "main"


class _VanishingInspector:
    """Delegates to a real inspector but raises ``NoSuchTableError`` when
    ``get_columns`` is asked for ``gone`` -- the table has been dropped
    since it was listed."""

    def __init__(self, insp, gone: str):
        self._insp = insp
        self._gone = gone

    def __getattr__(self, name):
        return getattr(self._insp, name)

    def get_columns(self, table_name, schema=None, **kwargs):
        if table_name == self._gone:
            raise NoSuchTableError(table_name)
        return self._insp.get_columns(table_name, schema=schema, **kwargs)


def _seed(tmp_path: Path) -> str:
    uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    eng = create_engine(uri)
    with eng.begin() as conn:
        conn.execute(sa_text("CREATE TABLE present (id INTEGER, name TEXT)"))
        conn.execute(sa_text("CREATE TABLE gone (id INTEGER)"))
    eng.dispose()
    return uri


def test_metadata_lists_all_tables(tmp_path):
    uri = _seed(tmp_path)
    entities = _SqliteExplorer(uri).metadata(None)
    assert {e.table for e in entities} == {"present", "gone"}


def test_metadata_skips_table_that_vanishes_mid_scan(tmp_path, monkeypatch):
    uri = _seed(tmp_path)

    real_inspect = explorer_mod.inspect
    monkeypatch.setattr(
        explorer_mod,
        "inspect",
        lambda eng: _VanishingInspector(real_inspect(eng), gone="gone"),
    )

    entities = _SqliteExplorer(uri).metadata(None)

    tables = {e.table for e in entities}
    assert tables == {"present"}
    present = next(e for e in entities if e.table == "present")
    assert {c.name for c in present.columns} == {"id", "name"}

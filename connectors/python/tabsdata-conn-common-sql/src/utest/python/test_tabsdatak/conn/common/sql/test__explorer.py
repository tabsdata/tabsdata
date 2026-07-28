#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for :class:`SqlAlchemyConnectionExplorer.metadata`.

Driven over an on-disk SQLite database so the real reflection path
(``inspect`` -> ``get_table_names`` -> ``get_columns``) runs without a
live server. The concurrent-DDL race is simulated by wrapping the
inspector so one listed table fails when reflected, as if it were dropped
between the table listing and the per-table reflection -- once as
``NoSuchTableError``, once as the raw driver error a dialect that does not
recognise its own missing-table code lets through.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text as sa_text
from sqlalchemy.exc import NoSuchTableError, ProgrammingError

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
    """Delegates to a real inspector but fails when ``get_columns`` is asked
    for ``gone`` -- the table has been dropped since it was listed."""

    def __init__(self, insp, gone: str, error: Exception | None = None):
        self._insp = insp
        self._gone = gone
        self._error = error or NoSuchTableError(gone)

    def __getattr__(self, name):
        return getattr(self._insp, name)

    def get_columns(self, table_name, schema=None, **kwargs):
        if table_name == self._gone:
            raise self._error
        return self._insp.get_columns(table_name, schema=schema, **kwargs)


def _driver_error(message: str) -> ProgrammingError:
    return ProgrammingError("SHOW CREATE TABLE gone", {}, Exception(message))


def _seed(tmp_path: Path) -> str:
    uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    eng = create_engine(uri)
    with eng.begin() as conn:
        conn.execute(sa_text("CREATE TABLE present (id INTEGER, name TEXT)"))
        conn.execute(sa_text("CREATE TABLE gone (id INTEGER)"))
    eng.dispose()
    return uri


def _vanish(monkeypatch, gone: str, error: Exception | None = None) -> None:
    real_inspect = explorer_mod.inspect
    monkeypatch.setattr(
        explorer_mod,
        "inspect",
        lambda eng: _VanishingInspector(real_inspect(eng), gone=gone, error=error),
    )


class _MissingTableExplorer(_SqliteExplorer):
    """A dialect that reports a dropped table with a driver error of its own
    instead of one SQLAlchemy maps to ``NoSuchTableError``."""

    def is_missing_table_error(self, error) -> bool:
        return "is not found" in str(error.orig)


def test_metadata_lists_all_tables(tmp_path):
    uri = _seed(tmp_path)
    entities = _SqliteExplorer(uri).metadata(None)
    assert {e.table for e in entities} == {"present", "gone"}


def test_metadata_skips_table_that_vanishes_mid_scan(tmp_path, monkeypatch):
    uri = _seed(tmp_path)
    _vanish(monkeypatch, gone="gone")

    entities = _SqliteExplorer(uri).metadata(None)

    tables = {e.table for e in entities}
    assert tables == {"present"}
    present = next(e for e in entities if e.table == "present")
    assert {c.name for c in present.columns} == {"id", "name"}


def test_metadata_skips_table_the_dialect_reports_gone(tmp_path, monkeypatch):
    uri = _seed(tmp_path)
    _vanish(monkeypatch, gone="gone", error=_driver_error("Table gone is not found."))

    entities = _MissingTableExplorer(uri).metadata(None)

    assert {e.table for e in entities} == {"present"}


def test_metadata_reraises_other_dialect_errors(tmp_path, monkeypatch):
    uri = _seed(tmp_path)
    _vanish(monkeypatch, gone="gone", error=_driver_error("connection reset"))

    with pytest.raises(ProgrammingError):
        _MissingTableExplorer(uri).metadata(None)


def test_metadata_reraises_a_dialect_error_by_default(tmp_path, monkeypatch):
    uri = _seed(tmp_path)
    _vanish(monkeypatch, gone="gone", error=_driver_error("Table gone is not found."))

    with pytest.raises(ProgrammingError):
        _SqliteExplorer(uri).metadata(None)

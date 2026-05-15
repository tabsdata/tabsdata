"""Sqlite-backed fixtures for the conn-common-sql ``_engine`` tests.

The engine layer (``SqlReadEngine`` / ``SqlWriteEngine``) is
deliberately ignorant of the underlying vendor: each connector
ships a ``SqlReadCfg`` / ``SqlWriteCfg`` subclass whose
``create_sql_alchemy_engine`` returns the right ``Engine``. The
adapter pair here is the test-only analogue -- it points a
sqlite-on-disk engine at a tmpdir-resident ``.db`` file so the
machinery (retry, transactions, polars / pandas readers, polars
writer, empty-result paths, ...) can be exercised end-to-end
without standing up a real database.

Tests reach into these helpers via the absolute path
``from test_tabsdatak.conn.common.sql._sqlite_helpers import ...``
to dodge clashing with the project's root ``conftest.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# noinspection PyProtectedMember
from tabsdatak.api import Conn
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.reader import SqlReadCfg
# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.writer import SqlWriteCfg


# ``SqlReadCfg`` / ``SqlWriteCfg`` are generic in the ``Conn``
# subclass. Sqlite is creditless, so a bare ``Conn`` placeholder
# works -- ``create_sql_alchemy_engine`` doesn't read from it.
class _NullConn(Conn):
    """Placeholder ``Conn`` that holds nothing the test engine
    needs. The sqlite engine is built from the ``uri`` field
    declared on the adapter itself."""


def sqlite_uri(db_path: Path) -> str:
    return f"sqlite:///{db_path}"


def build_engine(
        uri: str,
        *,
        connect_args: Mapping[str, Any] | None = None,
) -> Engine:
    """Minimal sqlite engine builder. Retry-path tests
    ``monkeypatch.setattr(_sqlite_helpers, "build_engine", ...)`` to
    simulate connect failures."""
    return create_engine(uri, connect_args=dict(connect_args or {}))


@dataclass(kw_only=True)
class _SqliteSqlReadCfg(SqlReadCfg[_NullConn]):
    """Test adapter for ``SqlReadCfg``. The sqlite URI is held on
    the adapter directly so each test can point at its own tmpdir
    ``.db``."""

    uri: str
    connect_args: dict[str, Any] | None = None
    # Defaults so individual tests only spell out what they care
    # about. Concrete connectors don't need these defaults.
    conn: _NullConn | None = None  # type: ignore[assignment]
    transactional: bool = True
    schema_overrides: Any = None
    query_params: Any = None
    cfg: Any = None

    def create_sql_alchemy_engine(self) -> Engine:
        # Resolve ``build_engine`` via the module each call so that
        # retry tests monkey-patching the symbol are honoured.
        from test_tabsdatak.conn.common.sql import _sqlite_helpers as _h
        return _h.build_engine(self.uri, connect_args=self.connect_args)


@dataclass(kw_only=True)
class _SqliteSqlWriteCfg(SqlWriteCfg[_NullConn]):
    uri: str
    connect_args: dict[str, Any] | None = None
    conn: _NullConn | None = None  # type: ignore[assignment]
    if_table_exists: Any = "append"
    transactional: bool = True
    cfg: Any = None

    def create_sql_alchemy_engine(self) -> Engine:
        from test_tabsdatak.conn.common.sql import _sqlite_helpers as _h
        return _h.build_engine(self.uri, connect_args=self.connect_args)

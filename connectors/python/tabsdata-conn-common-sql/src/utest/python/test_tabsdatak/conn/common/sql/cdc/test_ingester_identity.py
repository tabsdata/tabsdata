#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for schema resolution in
:meth:`~tabsdatak.conn.common.sql.cdc._ingester.CdcIngester.gather_table_identity`
-- in particular that an UNqualified table spec is resolved against the
connection's default schema (via :meth:`Dialect.default_schema`), matching how
batch SQL resolves an unqualified name, while an explicit ``schema.table`` spec
is honoured verbatim. These run without a database (the SQLAlchemy inspector is
mocked)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from tabsdatak.conn.common.sql.cdc._ingester import CdcIngester
from tabsdatak.conn.common.sql.cdc.dialect import Dialect
from tabsdatak.conn.common.sql.cdc.error import SqlCdcException


class _FakeDialect(Dialect):
    """A concrete dialect that relies on the base ``parse_qualified_table`` /
    ``default_schema`` implementations and stubs the rest. Double-quote
    identifier syntax; identity case-fold so assertions read literally."""

    open_quote = '"'
    close_quote = '"'

    def default_case_fold(self, name: str) -> str:
        return name

    def get_catalog_columns(self, connection, schema, table):  # pragma: no cover
        return []

    def is_large_column(self, satype: Any) -> bool:  # pragma: no cover
        return False

    def server_timezone(self, engine: Any):  # pragma: no cover
        return None


class _Ingester(CdcIngester):
    """Minimal concrete ingester exposing a fixed dialect; only
    ``gather_table_identity`` (inherited) is under test."""

    def __init__(self, dialect: Dialect):
        super().__init__("unused")
        self._dialect = dialect

    @property
    def ingester_type(self):  # pragma: no cover - not used here
        return None

    def close_session(self, session: Any) -> None:  # pragma: no cover
        pass

    @property
    def dialect(self) -> Dialect:
        return self._dialect

    def base_offset(self):  # pragma: no cover
        return None

    def commit_offset(self, offset) -> None:  # pragma: no cover
        pass

    def poll_changes(self, session: Any):  # pragma: no cover
        return []


def _inspector(default_schema="def_schema", has_table=True) -> MagicMock:
    inspector = MagicMock()
    inspector.default_schema_name = default_schema
    # normalize_name is identity for the test so assertions read literally.
    inspector.bind.dialect.normalize_name.side_effect = lambda n: str(n)
    inspector.has_table.return_value = has_table
    return inspector


def test_unqualified_defaults_to_connection_default_schema():
    ingester = _Ingester(_FakeDialect())
    inspector = _inspector(default_schema="def_schema")

    tracked = ingester.gather_table_identity("bare_table", inspector)

    assert tracked.data_table.schema == "def_schema"
    assert tracked.data_table.table == "bare_table"
    # the has-table probe used the defaulted schema
    _, kwargs = inspector.has_table.call_args
    assert kwargs["schema"] == "def_schema"


def test_qualified_spec_is_honoured_verbatim():
    ingester = _Ingester(_FakeDialect())
    inspector = _inspector(default_schema="def_schema")

    tracked = ingester.gather_table_identity("real.users", inspector)

    assert tracked.data_table.schema == "real"
    assert tracked.data_table.table == "users"


def test_unqualified_without_a_default_schema_raises_cdc_1():
    ingester = _Ingester(_FakeDialect())
    inspector = _inspector(default_schema=None)

    with pytest.raises(SqlCdcException):
        ingester.gather_table_identity("bare_table", inspector)


def test_base_parse_qualified_table_allows_unqualified():
    schema, table = _FakeDialect().parse_qualified_table("just_table")
    assert schema is None
    assert str(table) == "just_table"

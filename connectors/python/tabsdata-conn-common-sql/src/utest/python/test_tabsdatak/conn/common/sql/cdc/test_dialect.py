"""``Dialect`` abstract surface -- subclass enforcement + the
default ``cdc_to_sa`` NotImplementedError signal."""

from __future__ import annotations

import pytest

from tabsdatak.conn.common.sql.cdc.dialect import CdcIngesterType, Dialect


def test_dialect_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        Dialect()  # type: ignore[abstract]


class _PartialDialect(Dialect):
    # Implements every abstract method except ``cdc_to_sa`` (which
    # has a NotImplementedError default on the base) so we can
    # construct it.
    @property
    def open_quote(self):
        return '"'

    @property
    def close_quote(self):
        return '"'

    def default_case_fold(self, name: str) -> str:
        return name.lower()

    def parse_qualified_table(self, tablespec: str):
        from sqlalchemy.sql.elements import quoted_name
        schema, _, table = tablespec.partition(".")
        return quoted_name(schema, quote=False), quoted_name(table, quote=False)

    def get_catalog_columns(self, connection, schema, table):
        return []

    def is_large_column(self, satype) -> bool:
        return False

    def server_timezone(self, engine):
        return None


def test_partial_dialect_instantiates():
    d = _PartialDialect()
    assert d.default_case_fold("ABC") == "abc"


def test_cdc_to_sa_default_raises_notimplemented():
    d = _PartialDialect()
    with pytest.raises(NotImplementedError):
        d.cdc_to_sa("binlog", "INT")


def test_cdc_ingester_type_literal():
    # CdcIngesterType is a Literal; runtime values are plain str.
    val: CdcIngesterType = "binlog"
    assert val == "binlog"

#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect
from tabsdata.connector.sql.common._database.mssql.dialect import MSSQLDialect
from tabsdata.connector.sql.common._database.mysql.dialect import MySQLDialect
from tabsdata.connector.sql.common._database.oracle.dialect import OracleDialect
from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect

DOUBLE_QUOTE_DIALECTS = [
    PostgresDialect(),
    Db2Dialect(),
    OracleDialect(),
]

ALL_DIALECTS = [
    MySQLDialect(),
    MSSQLDialect(),
    *DOUBLE_QUOTE_DIALECTS,
]


@pytest.mark.parametrize(
    "dialect, spec, expected_schema, expected_table, schema_quoted, table_quoted",
    [
        *[
            (dialect, "schema.table", "schema", "table", False, False)
            for dialect in ALL_DIALECTS
        ],
        *[
            (dialect, "  schema.table  ", "schema", "table", False, False)
            for dialect in ALL_DIALECTS
        ],
        *[
            (dialect, "MY_SCHEMA.MY_TABLE", "MY_SCHEMA", "MY_TABLE", False, False)
            for dialect in ALL_DIALECTS
        ],
        *[
            (dialect, '"Schema"."Table"', "Schema", "Table", True, True)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, '"Schema".table', "Schema", "table", True, False)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, 'schema."Table"', "schema", "Table", False, True)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, '"a""b"."c"', 'a"b', "c", True, True)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, '"a""".table', 'a"', "table", True, False)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, '"""a"."b"', '"a', "b", True, True)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        *[
            (dialect, '"a""""b"."c"', 'a""b', "c", True, True)
            for dialect in DOUBLE_QUOTE_DIALECTS
        ],
        (MySQLDialect(), "`Schema`.`Table`", "Schema", "Table", True, True),
        (MySQLDialect(), "`Schema`.table", "Schema", "table", True, False),
        (MySQLDialect(), "schema.`Table`", "schema", "Table", False, True),
        (MySQLDialect(), "`a``b`.`c`", "a`b", "c", True, True),
        (MySQLDialect(), "`a```.table", "a`", "table", True, False),
        (MSSQLDialect(), "[Schema].[Table]", "Schema", "Table", True, True),
        (MSSQLDialect(), "[Schema].table", "Schema", "table", True, False),
        (MSSQLDialect(), "schema.[Table]", "schema", "Table", False, True),
        (MSSQLDialect(), "[a]]b].[c]", "a]b", "c", True, True),
        (MSSQLDialect(), "[a]]].[c]", "a]", "c", True, True),
    ],
    ids=lambda v: repr(v) if isinstance(v, str) else "",
)
def test_parse_valid(
    dialect, spec, expected_schema, expected_table, schema_quoted, table_quoted
):
    schema, table = dialect.parse_qualified_table(spec)
    assert str(schema) == expected_schema
    assert str(table) == expected_table
    assert schema.quote is schema_quoted
    assert table.quote is table_quoted


@pytest.mark.parametrize(
    "dialect, spec",
    [
        *[(d, "table_only") for d in ALL_DIALECTS],
        *[(d, "") for d in ALL_DIALECTS],
        *[(d, "   ") for d in ALL_DIALECTS],
        *[(d, ".table") for d in ALL_DIALECTS],
        *[(d, "schema.") for d in ALL_DIALECTS],
        *[(d, ".") for d in ALL_DIALECTS],
        *[(d, '"".table') for d in DOUBLE_QUOTE_DIALECTS],
        *[(d, 'schema.""') for d in DOUBLE_QUOTE_DIALECTS],
        (MySQLDialect(), "``.table"),
        (MySQLDialect(), "schema.``"),
        (MSSQLDialect(), "[].table"),
        (MSSQLDialect(), "schema.[]"),
        *[(d, '"schema.table') for d in DOUBLE_QUOTE_DIALECTS],
        *[(d, 'schema."table') for d in DOUBLE_QUOTE_DIALECTS],
        (MySQLDialect(), "`schema.table"),
        (MySQLDialect(), "schema.`table"),
        (MSSQLDialect(), "[schema.table"),
        (MSSQLDialect(), "schema.[table"),
        *[(d, 'a".b') for d in DOUBLE_QUOTE_DIALECTS],
        *[(d, 'a.b"') for d in DOUBLE_QUOTE_DIALECTS],
        (MySQLDialect(), "a`.b"),
        (MySQLDialect(), "a.b`"),
        (MSSQLDialect(), "a[.b"),
        (MSSQLDialect(), "a].b"),
        (MSSQLDialect(), "a.b["),
        (MSSQLDialect(), "a.b]"),
        *[(d, '"a"."b"extra') for d in DOUBLE_QUOTE_DIALECTS],
        *[(d, '"a"."b".c') for d in DOUBLE_QUOTE_DIALECTS],
        (MySQLDialect(), "`a`.`b`extra"),
        (MSSQLDialect(), "[a].[b]extra"),
        *[(d, '"a."b"') for d in DOUBLE_QUOTE_DIALECTS],
        (MySQLDialect(), "`a.`b`"),
    ],
    ids=lambda v: repr(v) if isinstance(v, str) else "",
)
def test_parse_invalid(dialect, spec):
    with pytest.raises(ValueError):
        dialect.parse_qualified_table(spec)

from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.mssql import (
    CredentialsSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    MSSQLDestConn,
    MSSQLSrcConn,
    MSSQLDest,
    MSSQLSrc,
    MSSQLTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.mssql.error import (
    MSSQLValidateException,
    MssqlErrorCode,
)


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# MSSQLConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [MSSQLSrcConn, MSSQLDestConn])
def test_mssql_conn_round_trip(cls):
    c = cls(
        uri="mssql://mssql.example.com:1433/analytics",
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
        conn_cfg={"mssql.timeout": 30},
    )
    loaded = cls.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "mssql://mssql.example.com:1433/analytics"
    assert loaded.conn_cfg == {"mssql.timeout": 30}


def test_mssql_src_dest_conn_have_distinct_kinds():
    assert MSSQLSrcConn.__kind__ != MSSQLDestConn.__kind__


def test_mssql_conn_accepts_odbc_connection_string_and_round_trips():
    odbc_uri = (
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=db.example.com,1433;"
        "Database=analytics;"
        "TrustServerCertificate=yes"
    )
    conn = MSSQLSrcConn(
        uri=odbc_uri,
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
    )
    loaded = MSSQLSrcConn.from_json(conn.to_json())
    assert loaded == conn
    assert loaded.uri == odbc_uri

    src = MSSQLSrc(queries=["SELECT 1"])
    assert MSSQLSrc.from_json(src.to_json()) == src

    dest = MSSQLDest(tables=["dbo.users"])
    assert MSSQLDest.from_json(dest.to_json()) == dest


@pytest.mark.parametrize("cls", [MSSQLSrcConn, MSSQLDestConn])
def test_mssql_conn_rejects_bad_uri(cls):
    # The ``uri`` field is a Union of ``str | Secret | Var``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        cls(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
        )


# ---------------------------------------------------------------------------
# MSSQLSrc
# ---------------------------------------------------------------------------


def test_mssql_src_full_round_trip():
    src = MSSQLSrc(
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        initial_values={"x": 1},
        src_cfg={"mssql.logging_level": 20},
    )
    loaded = MSSQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.initial_values == {"x": 1}
    assert loaded.src_cfg == {"mssql.logging_level": 20}


def test_mssql_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(MSSQLValidateException) as ei:
        MSSQLSrc(
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_3


def test_mssql_src_validate_accepts_matching_schema_overrides_length():
    src = MSSQLSrc(
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# MSSQLDest
# ---------------------------------------------------------------------------


def test_mssql_dest_full_round_trip():
    dest = MSSQLDest(
        tables=["dbo.users", "dbo.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"mssql.logging_level": 20},
    )
    loaded = MSSQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["dbo.users", "dbo.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"mssql.logging_level": 20}


def test_mssql_dest_defaults_if_table_exists_to_append():
    dest = MSSQLDest(tables=["dbo.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [MSSQLSrcConn, MSSQLDestConn])
def test_mssql_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "uri") == UriSpec
    assert _hint(cls, "credentials") == CredentialsSpec
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_mssql_src_fields_wire_expected_specs():
    assert _hint(MSSQLSrc, "queries") == QueriesSpec
    assert _hint(MSSQLSrc, "transactional") is bool
    assert _hint(MSSQLSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(MSSQLSrc, "src_cfg") == SrcCfgSpec | None


def test_mssql_dest_fields_wire_expected_specs():
    assert _hint(MSSQLDest, "tables") == MSSQLTableNamesSpec
    assert _hint(MSSQLDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(MSSQLDest, "transactional") is bool
    assert _hint(MSSQLDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_mssql_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MSSQLSrc(
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_2


def test_mssql_src_accepts_known_src_cfg_keys():
    src = MSSQLSrc(
        queries=["SELECT 1"],
        src_cfg={
            "mssql.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = MSSQLSrc.from_json(src.to_json())
    assert loaded == src


def test_mssql_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MSSQLDest(
            tables=["dbo.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_4


def test_mssql_dest_accepts_known_dest_cfg_keys():
    dest = MSSQLDest(
        tables=["dbo.users"],
        dest_cfg={
            "mssql.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = MSSQLDest.from_json(dest.to_json())
    assert loaded == dest


# ---------------------------------------------------------------------------
# MSSQLConn init validation error code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [MSSQLSrcConn, MSSQLDestConn])
def test_mssql_conn_rejects_missing_uri(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
        )
    assert ei.value.api_error is MssqlErrorCode.MSSQL_1


@pytest.mark.parametrize("cls", [MSSQLSrcConn, MSSQLDestConn])
def test_mssql_conn_rejects_missing_credentials(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(uri="mssql+pyodbc://h/d")  # type: ignore[call-arg]
    assert ei.value.api_error is MssqlErrorCode.MSSQL_1

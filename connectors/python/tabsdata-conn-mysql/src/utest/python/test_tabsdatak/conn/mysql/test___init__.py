from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword
from tabsdatak.conn.mysql import (
    ConnSpec,
    CredentialsSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    MySQLConn,
    MySQLDest,
    MySQLSrc,
    MySQLTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.mysql.error import (
    MySQLValidateException,
    MysqlErrorCode,
)


pytestmark = pytest.mark.mysql


def _conn() -> MySQLConn:
    return MySQLConn(
        uri="mysql://mysql.example.com:3306/analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"mysql.timeout": 30},
    )


# ---------------------------------------------------------------------------
# MySQLConn
# ---------------------------------------------------------------------------


def test_mysql_conn_round_trip():
    c = _conn()
    loaded = MySQLConn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "mysql://mysql.example.com:3306/analytics"
    assert loaded.conn_cfg == {"mysql.timeout": 30}


def test_mysql_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        MySQLConn(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# MySQLSrc
# ---------------------------------------------------------------------------


def test_mysql_src_full_round_trip():
    src = MySQLSrc(
        conn=_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"mysql.logging_level": 20},
    )
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"mysql.logging_level": 20}


def test_mysql_src_round_trip_with_named_conn():
    src = MySQLSrc(conn=NamedConn(name="prod_mysql"), queries=["SELECT 1"])
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mysql"


def test_mysql_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(MySQLValidateException) as ei:
        MySQLSrc(
            conn=_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_3


def test_mysql_src_validate_accepts_matching_schema_overrides_length():
    src = MySQLSrc(
        conn=_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# MySQLDest
# ---------------------------------------------------------------------------


def test_mysql_dest_full_round_trip():
    dest = MySQLDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"mysql.logging_level": 20},
    )
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"mysql.logging_level": 20}


def test_mysql_dest_round_trip_with_named_conn():
    dest = MySQLDest(conn=NamedConn(name="prod_mysql"), tables=["analytics.users"])
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_mysql"


def test_mysql_dest_defaults_if_table_exists_to_append():
    dest = MySQLDest(conn=_conn(), tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring -- assert each dataclass field carries the expected
# type alias. The aliases themselves (UriSpec, MySQLTableNamesSpec, ...)
# are exercised in test__validation.py and the conn-common test suite; here
# we only check the wiring, so the dataclasses can't silently drift away
# from their declared constraints.
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_mysql_conn_fields_wire_expected_specs():
    assert _hint(MySQLConn, "uri") == UriSpec
    assert _hint(MySQLConn, "credentials") == CredentialsSpec
    assert _hint(MySQLConn, "entity_whitelist") == EntityWhitelistSpec | None
    assert _hint(MySQLConn, "conn_cfg") == CfgSpec | None


def test_mysql_src_fields_wire_expected_specs():
    assert _hint(MySQLSrc, "conn") == ConnSpec
    assert _hint(MySQLSrc, "queries") == QueriesSpec
    assert _hint(MySQLSrc, "transactional") is bool
    assert _hint(MySQLSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(MySQLSrc, "src_cfg") == SrcCfgSpec | None


def test_mysql_dest_fields_wire_expected_specs():
    assert _hint(MySQLDest, "conn") == ConnSpec
    assert _hint(MySQLDest, "tables") == MySQLTableNamesSpec
    assert _hint(MySQLDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(MySQLDest, "transactional") is bool
    assert _hint(MySQLDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_mysql_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MySQLSrc(
            conn=_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_2


def test_mysql_src_accepts_known_src_cfg_keys():
    src = MySQLSrc(
        conn=_conn(),
        queries=["SELECT 1"],
        src_cfg={
            "mysql.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = MySQLSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.src_cfg == {
        "mysql.logging_level": 20,
        "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
        "tabsdata.sql.chunkSize": 100,
    }


def test_mysql_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        MySQLDest(
            conn=_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is MysqlErrorCode.MYSQL_4


def test_mysql_dest_accepts_known_dest_cfg_keys():
    dest = MySQLDest(
        conn=_conn(),
        tables=["analytics.users"],
        dest_cfg={
            "mysql.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = MySQLDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.dest_cfg == {
        "mysql.logging_level": 20,
        "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
        "tabsdata.sql.chunkSize": 100,
    }

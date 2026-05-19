from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.db2 import (
    ConnSpec,
    CredentialsSpec,
    Db2Conn,
    Db2Dest,
    Db2Src,
    Db2TableNamesSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.db2.error import (
    Db2ValidateException,
    Db2ErrorCode,
)


pytestmark = pytest.mark.db2


def _conn() -> Db2Conn:
    return Db2Conn(
        uri="db2://db2.example.com:50000/SAMPLE",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"db2.timeout": 30},
    )


# ---------------------------------------------------------------------------
# Db2Conn
# ---------------------------------------------------------------------------


def test_db2_conn_round_trip():
    c = _conn()
    loaded = Db2Conn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "db2://db2.example.com:50000/SAMPLE"
    assert loaded.conn_cfg == {"db2.timeout": 30}


def test_db2_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        Db2Conn(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# Db2Src
# ---------------------------------------------------------------------------


def test_db2_src_full_round_trip():
    src = Db2Src(
        conn=_conn(),
        queries=["SELECT * FROM USERS", "SELECT * FROM ORDERS"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"db2.logging_level": 20},
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM USERS", "SELECT * FROM ORDERS"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"db2.logging_level": 20}


def test_db2_src_round_trip_with_named_conn():
    src = Db2Src(
        conn=NamedConn(name="prod_db2"),
        queries=["SELECT 1 FROM SYSIBM.SYSDUMMY1"],
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_db2"


def test_db2_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(Db2ValidateException) as ei:
        Db2Src(
            conn=_conn(),
            queries=[
                "SELECT 1 FROM SYSIBM.SYSDUMMY1",
                "SELECT 2 FROM SYSIBM.SYSDUMMY1",
            ],
            schema_overrides=[None],
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_3


def test_db2_src_validate_accepts_matching_schema_overrides_length():
    src = Db2Src(
        conn=_conn(),
        queries=[
            "SELECT 1 FROM SYSIBM.SYSDUMMY1",
            "SELECT 2 FROM SYSIBM.SYSDUMMY1",
        ],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# Db2Dest
# ---------------------------------------------------------------------------


def test_db2_dest_full_round_trip():
    dest = Db2Dest(
        conn=_conn(),
        tables=["ANALYTICS.USERS", "ANALYTICS.ORDERS"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"db2.logging_level": 20},
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["ANALYTICS.USERS", "ANALYTICS.ORDERS"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"db2.logging_level": 20}


def test_db2_dest_round_trip_with_named_conn():
    dest = Db2Dest(
        conn=NamedConn(name="prod_db2"),
        tables=["ANALYTICS.USERS"],
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_db2"


def test_db2_dest_defaults_if_table_exists_to_append():
    dest = Db2Dest(conn=_conn(), tables=["ANALYTICS.USERS"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_db2_conn_fields_wire_expected_specs():
    assert _hint(Db2Conn, "uri") == UriSpec
    assert _hint(Db2Conn, "credentials") == CredentialsSpec
    assert _hint(Db2Conn, "conn_cfg") == CfgSpec | None


def test_db2_src_fields_wire_expected_specs():
    assert _hint(Db2Src, "conn") == ConnSpec
    assert _hint(Db2Src, "queries") == QueriesSpec
    assert _hint(Db2Src, "transactional") is bool
    assert _hint(Db2Src, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(Db2Src, "src_cfg") == SrcCfgSpec | None


def test_db2_dest_fields_wire_expected_specs():
    assert _hint(Db2Dest, "conn") == ConnSpec
    assert _hint(Db2Dest, "tables") == Db2TableNamesSpec
    assert _hint(Db2Dest, "if_table_exists") == IfTableExistsSpec
    assert _hint(Db2Dest, "transactional") is bool
    assert _hint(Db2Dest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_db2_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        Db2Src(
            conn=_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_2


def test_db2_src_accepts_known_src_cfg_keys():
    src = Db2Src(
        conn=_conn(),
        queries=["SELECT 1"],
        src_cfg={
            "db2.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = Db2Src.from_json(src.to_json())
    assert loaded == src


def test_db2_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        Db2Dest(
            conn=_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is Db2ErrorCode.DB2_4


def test_db2_dest_accepts_known_dest_cfg_keys():
    dest = Db2Dest(
        conn=_conn(),
        tables=["analytics.users"],
        dest_cfg={
            "db2.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = Db2Dest.from_json(dest.to_json())
    assert loaded == dest

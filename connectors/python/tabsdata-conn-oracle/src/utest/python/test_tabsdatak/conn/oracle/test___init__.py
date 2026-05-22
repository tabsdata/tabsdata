from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.oracle import (
    DestConnSpec,
    SrcConnSpec,
    CredentialsSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    OracleDestConn,
    OracleSrcConn,
    OracleDest,
    OracleSrc,
    OracleTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.oracle.error import (
    OracleValidateException,
    OracleErrorCode,
)


pytestmark = pytest.mark.oracle


def _src_conn() -> OracleSrcConn:
    return OracleSrcConn(
        uri="oracle://oracle.example.com:1521/orcl",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"oracle.timeout": 30},
    )


def _dest_conn() -> OracleDestConn:
    return OracleDestConn(
        uri="oracle://oracle.example.com:1521/orcl",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"oracle.timeout": 30},
    )


# ---------------------------------------------------------------------------
# OracleConn
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [OracleSrcConn, OracleDestConn])
def test_oracle_conn_round_trip(cls):
    c = cls(
        uri="oracle://oracle.example.com:1521/orcl",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"oracle.timeout": 30},
    )
    loaded = cls.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "oracle://oracle.example.com:1521/orcl"
    assert loaded.conn_cfg == {"oracle.timeout": 30}


def test_oracle_src_dest_conn_have_distinct_kinds():
    assert OracleSrcConn.__kind__ != OracleDestConn.__kind__


@pytest.mark.parametrize("cls", [OracleSrcConn, OracleDestConn])
def test_oracle_conn_rejects_bad_uri(cls):
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        cls(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# OracleSrc
# ---------------------------------------------------------------------------


def test_oracle_src_full_round_trip():
    src = OracleSrc(
        conn=_src_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"oracle.logging_level": 20},
    )
    loaded = OracleSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"oracle.logging_level": 20}


def test_oracle_src_round_trip_with_named_conn():
    src = OracleSrc(conn=NamedConn(name="prod_oracle"), queries=["SELECT 1"])
    loaded = OracleSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_oracle"


def test_oracle_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(OracleValidateException) as ei:
        OracleSrc(
            conn=_src_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_3


def test_oracle_src_validate_accepts_matching_schema_overrides_length():
    src = OracleSrc(
        conn=_src_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# OracleDest
# ---------------------------------------------------------------------------


def test_oracle_dest_full_round_trip():
    dest = OracleDest(
        conn=_dest_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"oracle.logging_level": 20},
    )
    loaded = OracleDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"oracle.logging_level": 20}


def test_oracle_dest_round_trip_with_named_conn():
    dest = OracleDest(conn=NamedConn(name="prod_oracle"), tables=["analytics.users"])
    loaded = OracleDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_oracle"


def test_oracle_dest_defaults_if_table_exists_to_append():
    dest = OracleDest(conn=_dest_conn(), tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


@pytest.mark.parametrize("cls", [OracleSrcConn, OracleDestConn])
def test_oracle_conn_fields_wire_expected_specs(cls):
    assert _hint(cls, "uri") == UriSpec
    assert _hint(cls, "credentials") == CredentialsSpec
    assert _hint(cls, "conn_cfg") == CfgSpec | None


def test_oracle_src_fields_wire_expected_specs():
    assert _hint(OracleSrc, "conn") == SrcConnSpec
    assert _hint(OracleSrc, "queries") == QueriesSpec
    assert _hint(OracleSrc, "transactional") is bool
    assert _hint(OracleSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(OracleSrc, "src_cfg") == SrcCfgSpec | None


def test_oracle_dest_fields_wire_expected_specs():
    assert _hint(OracleDest, "conn") == DestConnSpec
    assert _hint(OracleDest, "tables") == OracleTableNamesSpec
    assert _hint(OracleDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(OracleDest, "transactional") is bool
    assert _hint(OracleDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_oracle_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        OracleSrc(
            conn=_src_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_2


def test_oracle_src_accepts_known_src_cfg_keys():
    src = OracleSrc(
        conn=_src_conn(),
        queries=["SELECT 1"],
        src_cfg={
            "oracle.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = OracleSrc.from_json(src.to_json())
    assert loaded == src


def test_oracle_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        OracleDest(
            conn=_dest_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_4


def test_oracle_dest_accepts_known_dest_cfg_keys():
    dest = OracleDest(
        conn=_dest_conn(),
        tables=["analytics.users"],
        dest_cfg={
            "oracle.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = OracleDest.from_json(dest.to_json())
    assert loaded == dest


# ---------------------------------------------------------------------------
# OracleConn init validation error code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cls", [OracleSrcConn, OracleDestConn])
def test_oracle_conn_rejects_missing_uri(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(  # type: ignore[call-arg]
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_1


@pytest.mark.parametrize("cls", [OracleSrcConn, OracleDestConn])
def test_oracle_conn_rejects_missing_credentials(cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        cls(uri="oracle+oracledb://h:1521/db")  # type: ignore[call-arg]
    assert ei.value.api_error is OracleErrorCode.ORACLE_1

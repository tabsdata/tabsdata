from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.sql.types import SchemaOverrideSpec
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.postgres import (
    ConnSpec,
    CredentialsSpec,
    DestCfgSpec,
    IfTableExistsSpec,
    PostgresConn,
    PostgresDest,
    PostgresSrc,
    PostgresTableNamesSpec,
    QueriesSpec,
    SrcCfgSpec,
    UriSpec,
)
# noinspection PyProtectedMember
from tabsdatak.conn.postgres.error import (
    PostgresValidateException,
    PostgresErrorCode,
)


pytestmark = pytest.mark.postgres


def _conn() -> PostgresConn:
    return PostgresConn(
        uri="postgresql://pg.example.com:5432/analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"postgres.timeout": 30},
    )


# ---------------------------------------------------------------------------
# PostgresConn
# ---------------------------------------------------------------------------


def test_postgres_conn_round_trip():
    c = _conn()
    loaded = PostgresConn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "postgresql://pg.example.com:5432/analytics"
    assert loaded.conn_cfg == {"postgres.timeout": 30}


def test_postgres_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        PostgresConn(
            uri="ftp://host/db",
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )


# ---------------------------------------------------------------------------
# PostgresSrc
# ---------------------------------------------------------------------------


def test_postgres_src_full_round_trip():
    src = PostgresSrc(
        conn=_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"postgres.logging_level": 20},
    )
    loaded = PostgresSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"postgres.logging_level": 20}


def test_postgres_src_round_trip_with_named_conn():
    src = PostgresSrc(conn=NamedConn(name="prod_postgres"), queries=["SELECT 1"])
    loaded = PostgresSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_postgres"


def test_postgres_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(PostgresValidateException) as ei:
        PostgresSrc(
            conn=_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_3


def test_postgres_src_validate_accepts_matching_schema_overrides_length():
    src = PostgresSrc(
        conn=_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# PostgresDest
# ---------------------------------------------------------------------------


def test_postgres_dest_full_round_trip():
    dest = PostgresDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"postgres.logging_level": 20},
    )
    loaded = PostgresDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"postgres.logging_level": 20}


def test_postgres_dest_round_trip_with_named_conn():
    dest = PostgresDest(conn=NamedConn(name="prod_postgres"), tables=["analytics.users"])
    loaded = PostgresDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_postgres"


def test_postgres_dest_defaults_if_table_exists_to_append():
    dest = PostgresDest(conn=_conn(), tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_postgres_conn_fields_wire_expected_specs():
    assert _hint(PostgresConn, "uri") == UriSpec
    assert _hint(PostgresConn, "credentials") == CredentialsSpec
    assert _hint(PostgresConn, "conn_cfg") == CfgSpec | None


def test_postgres_src_fields_wire_expected_specs():
    assert _hint(PostgresSrc, "conn") == ConnSpec
    assert _hint(PostgresSrc, "queries") == QueriesSpec
    assert _hint(PostgresSrc, "transactional") is bool
    assert _hint(PostgresSrc, "schema_overrides") == SchemaOverrideSpec | None
    assert _hint(PostgresSrc, "src_cfg") == SrcCfgSpec | None


def test_postgres_dest_fields_wire_expected_specs():
    assert _hint(PostgresDest, "conn") == ConnSpec
    assert _hint(PostgresDest, "tables") == PostgresTableNamesSpec
    assert _hint(PostgresDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(PostgresDest, "transactional") is bool
    assert _hint(PostgresDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# src_cfg / dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_postgres_src_rejects_unknown_src_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        PostgresSrc(
            conn=_conn(),
            queries=["SELECT 1"],
            src_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_2


def test_postgres_src_accepts_known_src_cfg_keys():
    src = PostgresSrc(
        conn=_conn(),
        queries=["SELECT 1"],
        src_cfg={
            "postgres.logging_level": 20,
            "tabsdata.sql.sql.readEngine": "polarsSqlAlchemyRead",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = PostgresSrc.from_json(src.to_json())
    assert loaded == src


def test_postgres_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        PostgresDest(
            conn=_conn(),
            tables=["analytics.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_4


def test_postgres_dest_accepts_known_dest_cfg_keys():
    dest = PostgresDest(
        conn=_conn(),
        tables=["analytics.users"],
        dest_cfg={
            "postgres.logging_level": 20,
            "tabsdata.sql.sql.writeEngine": "polarsSqlAlchemyWrite",
            "tabsdata.sql.chunkSize": 100,
        },
    )
    loaded = PostgresDest.from_json(dest.to_json())
    assert loaded == dest

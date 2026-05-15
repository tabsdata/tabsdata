from __future__ import annotations

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.oracle import OracleConn, OracleDest, OracleSrc
# noinspection PyProtectedMember
from tabsdatak.conn.oracle.error import (
    OracleValidateException,
    OracleErrorCode,
)


pytestmark = pytest.mark.oracle


def _conn() -> OracleConn:
    return OracleConn(
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


def test_oracle_conn_round_trip():
    c = _conn()
    loaded = OracleConn.from_json(c.to_json())
    assert loaded == c
    assert loaded.uri == "oracle://oracle.example.com:1521/orcl"
    assert loaded.conn_cfg == {"oracle.timeout": 30}


def test_oracle_conn_rejects_bad_uri():
    # The ``uri`` field is a Union of ``str | Secret | Env``, each
    # carrying the same regex. A bad value fails *every* branch of the
    # Union -- pydantic surfaces whichever raised; assert on the
    # ``TabsdataException`` umbrella rather than the specific code.
    with pytest.raises(TabsdataException):
        OracleConn(
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
        conn=_conn(),
        queries=["SELECT * FROM users", "SELECT * FROM orders"],
        transactional=False,
        schema_overrides=[None, None],
        query_params={"x": 1},
        src_cfg={"chunkSize": 1024},
    )
    loaded = OracleSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.queries == ["SELECT * FROM users", "SELECT * FROM orders"]
    assert loaded.transactional is False
    assert loaded.schema_overrides == [None, None]
    assert loaded.query_params == {"x": 1}
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_oracle_src_round_trip_with_named_conn():
    src = OracleSrc(conn=NamedConn(name="prod_oracle"), queries=["SELECT 1"])
    loaded = OracleSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_oracle"


def test_oracle_src_validate_rejects_schema_overrides_length_mismatch():
    with pytest.raises(OracleValidateException) as ei:
        OracleSrc(
            conn=_conn(),
            queries=["SELECT 1", "SELECT 2"],
            schema_overrides=[None],
        )
    assert ei.value.api_error is OracleErrorCode.ORACLE_3


def test_oracle_src_validate_accepts_matching_schema_overrides_length():
    src = OracleSrc(
        conn=_conn(),
        queries=["SELECT 1", "SELECT 2"],
        schema_overrides=[None, None],
    )
    assert len(src.schema_overrides) == len(src.queries)


# ---------------------------------------------------------------------------
# OracleDest
# ---------------------------------------------------------------------------


def test_oracle_dest_full_round_trip():
    dest = OracleDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        transactional=False,
        dest_cfg={"chunkSize": 2048},
    )
    loaded = OracleDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.transactional is False
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_oracle_dest_round_trip_with_named_conn():
    dest = OracleDest(conn=NamedConn(name="prod_oracle"), tables=["analytics.users"])
    loaded = OracleDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_oracle"


def test_oracle_dest_rejects_bad_table_name():
    with pytest.raises(ConnInitException) as ei:
        OracleDest(
            conn=_conn(),
            tables=["bad-name"],
        )
    assert ei.value.api_error in {OracleErrorCode.ORACLE_4, OracleErrorCode.ORACLE_15}


def test_oracle_dest_defaults_if_table_exists_to_append():
    dest = OracleDest(conn=_conn(), tables=["analytics.users"])
    assert dest.if_table_exists == "append"
    assert dest.transactional is True
    assert dest.dest_cfg is None

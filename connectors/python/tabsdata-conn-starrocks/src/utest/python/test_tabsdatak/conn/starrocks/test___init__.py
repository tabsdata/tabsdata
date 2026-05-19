from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Env, NamedConn, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    CfgSpec,
    UserPassword,
)
from tabsdatak.conn.starrocks import (
    ConnSpec,
    CredentialsSpec,
    DatabaseSpec,
    DestCfgSpec,
    HostSpec,
    IfTableExistsSpec,
    S3Location,
    PortSpec,
    SchemaEvolutionSpec,
    StarRocksConn,
    StarRocksDest,
    StarRocksTableNamesSpec,
)
from tabsdatak.conn.starrocks.error import StarRocksErrorCode
from tabsdatak.conn.common.error import ConnInitException


pytestmark = pytest.mark.starrocks


def _staging():
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        base_path="staging/",
    )


def _conn():
    return StarRocksConn(
        host="starrocks.example.com",
        staging=_staging(),
        port=9031,
        database="analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"starrocks.timeout": 30},
    )


def test_starrocks_dest_full_round_trip():
    dest = StarRocksDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        schema_evolution="iceberg",
        dest_cfg={"starrocks.logging_level": 20},
    )
    loaded = StarRocksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.port == 9031
    assert loaded.conn.database.value() == "analytics"
    assert loaded.conn.conn_cfg == {"starrocks.timeout": 30}
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "iceberg"
    assert loaded.dest_cfg == {"starrocks.logging_level": 20}


def test_starrocks_dest_round_trip_with_named_conn():
    dest = StarRocksDest(
        conn=NamedConn(name="prod_starrocks"),
        tables=["analytics.users"],
    )
    loaded = StarRocksDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_starrocks"


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_starrocks_conn_fields_wire_expected_specs():
    assert _hint(StarRocksConn, "host") == HostSpec
    assert _hint(StarRocksConn, "port") == PortSpec
    assert _hint(StarRocksConn, "database") == DatabaseSpec
    assert _hint(StarRocksConn, "credentials") == CredentialsSpec
    assert _hint(StarRocksConn, "conn_cfg") == CfgSpec | None


def test_starrocks_dest_fields_wire_expected_specs():
    assert _hint(StarRocksDest, "conn") == ConnSpec
    assert _hint(StarRocksDest, "tables") == StarRocksTableNamesSpec
    assert _hint(StarRocksDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(StarRocksDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(StarRocksDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# StarRocksConn._port() casting helper
# ---------------------------------------------------------------------------


def _conn_with_port(port):
    return StarRocksConn(
        host="sr.example.com",
        staging=_staging(),
        port=port,
        credentials=UserPassword(
            user=Secret.lit("u"), password=Secret.lit("p"),
        ),
    )


def test_port_method_returns_int_when_int():
    assert _conn_with_port(9031)._port() == 9031


def test_port_method_resolves_env(monkeypatch):
    monkeypatch.setenv("SR_PORT", "9101")
    assert _conn_with_port(Env("SR_PORT"))._port() == 9101


def test_port_method_resolves_secret_literal():
    assert _conn_with_port(Secret.lit("9102"))._port() == 9102


def test_port_env_non_numeric_rejected_at_value(monkeypatch):
    monkeypatch.setenv("SR_PORT_BAD", "notanumber")
    conn = _conn_with_port(Env("SR_PORT_BAD"))
    with pytest.raises(Exception):
        conn._port()


def test_port_secret_literal_non_numeric_rejected_at_construction():
    with pytest.raises(Exception):
        _conn_with_port(Secret.lit("notanumber"))


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def _bare_conn():
    return StarRocksConn(
        host=Secret.lit("sr.example.com"),
        staging=S3Location(
            bucket="staging",
            base_path="/foo",
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret.lit("ak"),
                secret_access_key=Secret.lit("sk"),
            ),
        ),
        credentials=UserPassword(
            user=Secret.lit("u"),
            password=Secret.lit("p"),
        ),
    )


def test_starrocks_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        StarRocksDest(
            conn=_bare_conn(),
            tables=["default.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_2


def test_starrocks_dest_accepts_known_dest_cfg_keys():
    dest = StarRocksDest(
        conn=_bare_conn(),
        tables=["default.users"],
        dest_cfg={"starrocks.logging_level": 20},
    )
    assert dest.dest_cfg == {"starrocks.logging_level": 20}

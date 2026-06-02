from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Var, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    CfgSpec,
    UserPassword,
)
from tabsdatak.conn.starrocks import (
    CredentialsSpec,
    DatabaseSpec,
    DestCfgSpec,
    HostSpec,
    IfTableExistsSpec,
    S3Location,
    PortSpec,
    SchemaEvolutionSpec,
    StarRocksDestConn,
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


def test_starrocks_dest_full_round_trip():
    dest = StarRocksDest(
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        schema_evolution="iceberg",
        dest_cfg={"starrocks.logging_level": 20},
    )
    loaded = StarRocksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "iceberg"
    assert loaded.dest_cfg == {"starrocks.logging_level": 20}


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_starrocks_conn_fields_wire_expected_specs():
    assert _hint(StarRocksDestConn, "host") == HostSpec
    assert _hint(StarRocksDestConn, "port") == PortSpec
    assert _hint(StarRocksDestConn, "database") == DatabaseSpec
    assert _hint(StarRocksDestConn, "credentials") == CredentialsSpec
    assert _hint(StarRocksDestConn, "conn_cfg") == CfgSpec | None


def test_starrocks_dest_fields_wire_expected_specs():
    assert _hint(StarRocksDest, "tables") == StarRocksTableNamesSpec
    assert _hint(StarRocksDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(StarRocksDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(StarRocksDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# port resolution via _convert at the use site
# ---------------------------------------------------------------------------


def _conn_with_port(port):
    return StarRocksDestConn(
        host="sr.example.com",
        staging=_staging(),
        port=port,
        credentials=UserPassword(
            user=Secret.lit("u"), password=Secret.lit("p"),
        ),
    )


def test_port_resolves_int():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert _convert(_conn_with_port(9031).port, int) == 9031




def test_port_resolves_secret_literal():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert _convert(_conn_with_port(Secret.lit("9102")).port, int) == 9102


def test_port_env_non_numeric_rejected_at_value(monkeypatch):
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    monkeypatch.setenv("SR_PORT_BAD", "notanumber")
    conn = _conn_with_port(Var._var_key("SR_PORT_BAD"))
    with pytest.raises(Exception):
        _convert(conn.port, int)


def test_port_secret_literal_non_numeric_rejected_at_construction():
    with pytest.raises(Exception):
        _conn_with_port(Secret.lit("notanumber"))


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def _bare_conn():
    return StarRocksDestConn(
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
            tables=["default.users"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_2


def test_starrocks_dest_accepts_known_dest_cfg_keys():
    dest = StarRocksDest(
        tables=["default.users"],
        dest_cfg={"starrocks.logging_level": 20},
    )
    assert dest.dest_cfg == {"starrocks.logging_level": 20}


def test_starrocks_conn_rejects_missing_required_fields():
    with pytest.raises(ConnInitException) as ei:
        StarRocksDestConn(  # type: ignore[call-arg]
            staging=_staging(),
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_1

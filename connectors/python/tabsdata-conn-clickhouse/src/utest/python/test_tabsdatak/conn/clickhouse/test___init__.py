from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Var, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    CfgSpec,
    GCPHmac,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.clickhouse import (
    ClickHouseDestConn,
    ClickHouseDest,
    ClickHouseTableNamesSpec,
    CloudLocation,
    CredentialsSpec,
    DatabaseSpec,
    DestCfgSpec,
    GCSLocation,
    HostSpec,
    IfTableExistsSpec,
    PortSpec,
    S3Location,
    SchemaEvolutionSpec,
    SecureSpec,
)
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode


pytestmark = pytest.mark.clickhouse


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


def test_clickhouse_dest_full_round_trip():
    dest = ClickHouseDest(
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        schema_evolution="iceberg",
        dest_cfg={"clickhouse.logging_level": 20},
    )
    loaded = ClickHouseDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "iceberg"
    assert loaded.dest_cfg == {"clickhouse.logging_level": 20}


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_clickhouse_conn_fields_wire_expected_specs():
    assert _hint(ClickHouseDestConn, "host") == HostSpec
    assert _hint(ClickHouseDestConn, "staging") == CloudLocation
    assert _hint(ClickHouseDestConn, "port") == PortSpec
    assert _hint(ClickHouseDestConn, "database") == DatabaseSpec
    assert _hint(ClickHouseDestConn, "credentials") == CredentialsSpec
    assert _hint(ClickHouseDestConn, "secure") == SecureSpec
    assert _hint(ClickHouseDestConn, "conn_cfg") == CfgSpec | None


def test_clickhouse_dest_fields_wire_expected_specs():
    assert _hint(ClickHouseDest, "tables") == ClickHouseTableNamesSpec
    assert _hint(ClickHouseDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(ClickHouseDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(ClickHouseDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# port / secure resolution via _convert at the use site
# ---------------------------------------------------------------------------


def _bare_conn(*, port, secure=False):
    return ClickHouseDestConn(
        host="ch.example.com",
        staging=_staging(),
        port=port,
        secure=secure,
        credentials=UserPassword(
            user=Secret.lit("u"), password=Secret.lit("p"),
        ),
    )


def test_port_resolves_int():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert _convert(_bare_conn(port=8443).port, int) == 8443




def test_port_resolves_secret_literal():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert _convert(_bare_conn(port=Secret.lit("9002")).port, int) == 9002


def test_port_env_non_numeric_rejected_at_value(monkeypatch):
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    monkeypatch.setenv("CH_PORT_BAD", "notanumber")
    conn = _bare_conn(port=Var._var_key("CH_PORT_BAD"))
    with pytest.raises(Exception):
        _convert(conn.port, int)


def test_port_secret_literal_non_numeric_rejected_at_construction():
    # The literal's eager pattern check fires at construction time.
    with pytest.raises(Exception):
        _bare_conn(port=Secret.lit("notanumber"))


def test_secure_resolves_bool():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert _convert(_bare_conn(port=1, secure=True).secure, bool) is True
    assert _convert(_bare_conn(port=1, secure=False).secure, bool) is False






def test_secure_resolves_secret_literal():
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.types import _convert

    assert (
        _convert(_bare_conn(port=1, secure=Secret.lit("true")).secure, bool)
        is True
    )
    assert (
        _convert(_bare_conn(port=1, secure=Secret.lit("false")).secure, bool)
        is False
    )


def test_secure_secret_literal_invalid_token_rejected_at_construction():
    with pytest.raises(Exception):
        _bare_conn(port=1, secure=Secret.lit("maybe"))


# ---------------------------------------------------------------------------
# GCSLocation requires both credential types
# ---------------------------------------------------------------------------


def _sa_creds() -> GCPJson:
    return GCPJson(json=Secret.lit('{"type":"service_account"}'))


def _hmac_creds() -> GCPHmac:
    return GCPHmac(
        access_key=Secret.lit("GOOG-AK"),
        secret_key=Secret.lit("GOOG-SK"),
    )


def test_gcs_location_accepts_both_credential_types():
    loc = GCSLocation(
        bucket="my-bucket",
        credentials=_sa_creds(),
        hmac_credentials=_hmac_creds(),
        base_path="staging",
    )
    assert isinstance(loc.credentials, GCPJson)
    assert isinstance(loc.hmac_credentials, GCPHmac)


def test_gcs_location_round_trips_json():
    original = GCSLocation(
        bucket="my-bucket",
        credentials=_sa_creds(),
        hmac_credentials=_hmac_creds(),
        base_path="staging/clickhouse",
    )
    loaded = GCSLocation.from_json(original.to_json())
    assert loaded == original


def test_gcs_location_rejects_missing_service_account():
    with pytest.raises(Exception) as ei:
        GCSLocation(
            bucket="my-bucket",
            hmac_credentials=_hmac_creds(),
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_10


def test_gcs_location_rejects_missing_hmac():
    with pytest.raises(Exception) as ei:
        GCSLocation(
            bucket="my-bucket",
            credentials=_sa_creds(),
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_10


def test_gcs_location_rejects_swapped_credentials():
    """SA JSON in the HMAC slot (or vice versa) is a static type
    error and a runtime construction failure -- pydantic rejects
    the mismatched ``__kind__``."""
    with pytest.raises(Exception) as ei:
        GCSLocation(
            bucket="my-bucket",
            credentials=_hmac_creds(),   # wrong slot
            hmac_credentials=_sa_creds(),  # wrong slot
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_10


def test_gcs_location_field_specs():
    assert _hint(GCSLocation, "credentials") == GCPJson
    assert _hint(GCSLocation, "hmac_credentials") == GCPHmac


def test_clickhouse_conn_rejects_missing_required_fields():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        ClickHouseDestConn(  # type: ignore[call-arg]
            staging=_staging(),
            credentials=UserPassword(
                user=Secret.lit("u"),
                password=Secret.lit("p"),
            ),
        )
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_1

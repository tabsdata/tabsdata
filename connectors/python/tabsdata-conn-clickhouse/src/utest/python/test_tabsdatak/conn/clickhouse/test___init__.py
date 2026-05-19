from __future__ import annotations

import typing

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    CfgSpec,
    GCPHmac,
    GCPJson,
    UserPassword,
)
from tabsdatak.conn.clickhouse import (
    ClickHouseConn,
    ClickHouseDest,
    ClickHouseTableNamesSpec,
    CloudLocation,
    ConnSpec,
    CredentialsSpec,
    DatabaseSpec,
    DestCfgSpec,
    GCSLocation,
    HostSpec,
    IfTableExistsSpec,
    PortSpec,
    S3Location,
    SchemaEvolutionSpec,
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


def _conn():
    return ClickHouseConn(
        host="ch.example.com",
        staging=_staging(),
        port=8443,
        database="analytics",
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        secure=True,
        conn_cfg={"clickhouse.timeout": 30},
    )


def test_clickhouse_dest_full_round_trip():
    dest = ClickHouseDest(
        conn=_conn(),
        tables=["analytics.users", "analytics.orders"],
        if_table_exists="replace",
        schema_evolution="iceberg",
        dest_cfg={"clickhouse.logging_level": 20},
    )
    loaded = ClickHouseDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.port == 8443
    assert loaded.conn.database.value() == "analytics"
    assert loaded.conn.secure is True
    assert loaded.conn.conn_cfg == {"clickhouse.timeout": 30}
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "iceberg"
    assert loaded.dest_cfg == {"clickhouse.logging_level": 20}


def test_clickhouse_dest_round_trip_with_named_conn():
    dest = ClickHouseDest(
        conn=NamedConn(name="prod_clickhouse"),
        tables=["analytics.users"],
    )
    loaded = ClickHouseDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_clickhouse"


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_clickhouse_conn_fields_wire_expected_specs():
    assert _hint(ClickHouseConn, "host") == HostSpec
    assert _hint(ClickHouseConn, "staging") == CloudLocation
    assert _hint(ClickHouseConn, "port") == PortSpec
    assert _hint(ClickHouseConn, "database") == DatabaseSpec
    assert _hint(ClickHouseConn, "credentials") == CredentialsSpec
    assert _hint(ClickHouseConn, "secure") is bool
    assert _hint(ClickHouseConn, "conn_cfg") == CfgSpec | None


def test_clickhouse_dest_fields_wire_expected_specs():
    assert _hint(ClickHouseDest, "conn") == ConnSpec
    assert _hint(ClickHouseDest, "tables") == ClickHouseTableNamesSpec
    assert _hint(ClickHouseDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(ClickHouseDest, "schema_evolution") == SchemaEvolutionSpec
    assert _hint(ClickHouseDest, "dest_cfg") == DestCfgSpec | None


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

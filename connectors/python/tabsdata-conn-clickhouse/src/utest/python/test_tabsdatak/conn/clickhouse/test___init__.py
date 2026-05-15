
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    S3Location,
    UserPassword,
)
from tabsdatak.conn.clickhouse import ClickHouseConn, ClickHouseDest


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
        dest_cfg={"chunkSize": 2048},
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
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_clickhouse_dest_round_trip_with_named_conn():
    dest = ClickHouseDest(
        conn=NamedConn(name="prod_clickhouse"),
        tables=["analytics.users"],
    )
    loaded = ClickHouseDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_clickhouse"

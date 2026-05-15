
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    S3Location,
    UserPassword,
)
from tabsdatak.conn.starrocks import StarRocksConn, StarRocksDest


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
        dest_cfg={"chunkSize": 2048},
    )
    loaded = StarRocksDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.port == 9031
    assert loaded.conn.database.value() == "analytics"
    assert loaded.conn.conn_cfg == {"starrocks.timeout": 30}
    assert loaded.tables == ["analytics.users", "analytics.orders"]
    assert loaded.if_table_exists == "replace"
    assert loaded.schema_evolution == "iceberg"
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_starrocks_dest_round_trip_with_named_conn():
    dest = StarRocksDest(
        conn=NamedConn(name="prod_starrocks"),
        tables=["analytics.users"],
    )
    loaded = StarRocksDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_starrocks"

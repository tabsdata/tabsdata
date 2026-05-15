
import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    S3Location,
    UserPassword,
)
from tabsdatak.conn.druid import DruidConn, DruidDest


pytestmark = pytest.mark.druid


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
    return DruidConn(
        overlord_url="http://druid.example.com:8081",
        staging=_staging(),
        credentials=UserPassword(
            user=Secret.lit("etl"),
            password=Secret.lit("hunter2"),
        ),
        conn_cfg={"druid.timeout": 30},
    )


def test_druid_dest_full_round_trip():
    dest = DruidDest(
        conn=_conn(),
        tables=["events", "metrics"],
        if_table_exists="replace",
        time_column="ts",
        segment_granularity="HOUR",
        max_concurrent_subtasks=4,
        max_rows_per_segment=2_000_000,
        max_rows_in_memory=500_000,
        http_timeout_sec=120,
        dest_cfg={"chunkSize": 4096},
    )
    loaded = DruidDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.conn_cfg == {"druid.timeout": 30}
    assert loaded.tables == ["events", "metrics"]
    assert loaded.if_table_exists == "replace"
    assert loaded.time_column == "ts"
    assert loaded.segment_granularity == "HOUR"
    assert loaded.max_concurrent_subtasks == 4
    assert loaded.max_rows_per_segment == 2_000_000
    assert loaded.max_rows_in_memory == 500_000
    assert loaded.http_timeout_sec == 120
    assert loaded.dest_cfg == {"chunkSize": 4096}


def test_druid_dest_round_trip_with_named_conn():
    dest = DruidDest(
        conn=NamedConn(name="prod_druid"),
        tables=["events"],
    )
    loaded = DruidDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_druid"

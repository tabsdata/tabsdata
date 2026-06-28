from __future__ import annotations

import typing

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    CfgSpec,
    UserPassword,
)
from tabsdatak.conn.druid import (
    CloudLocation,
    S3Location,
)
from tabsdatak.conn.druid import (
    CredentialsSpec,
    DestCfgSpec,
    DruidDestConn,
    DruidDest,
    DruidTableNamesSpec,
    IfTableExistsSpec,
    SegmentGranularitySpec,
    UrlSpec,
)
from tabsdatak.conn.druid.error import DruidErrorCode
from tabsdatak.conn.common.error import ConnInitException


pytestmark = pytest.mark.druid


def _staging():
    return S3Location(
        bucket="staging-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret("AKIA..."),
            secret_access_key=Secret("secret"),
        ),
        base_path="staging/",
    )


def _conn():
    return DruidDestConn(
        overlord_url="http://druid.example.com:8081",
        staging=_staging(),
        credentials=UserPassword(
            user=Secret("etl"),
            password=Secret("hunter2"),
        ),
        conn_cfg={"druid.timeout": 30},
    )


def test_druid_dest_full_round_trip():
    dest = DruidDest(
        tables=["events", "metrics"],
        if_table_exists="replace",
        time_column="ts",
        segment_granularity="HOUR",
        max_concurrent_subtasks=4,
        max_rows_per_segment=2_000_000,
        max_rows_in_memory=500_000,
        http_timeout_sec=120,
        dest_cfg={"druid.logging_level": 20},
    )
    loaded = DruidDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.tables == ["events", "metrics"]
    assert loaded.if_table_exists == "replace"
    assert loaded.time_column == "ts"
    assert loaded.segment_granularity == "HOUR"
    assert loaded.max_concurrent_subtasks == 4
    assert loaded.max_rows_per_segment == 2_000_000
    assert loaded.max_rows_in_memory == 500_000
    assert loaded.http_timeout_sec == 120
    assert loaded.dest_cfg == {"druid.logging_level": 20}


# ---------------------------------------------------------------------------
# Field-spec wiring
# ---------------------------------------------------------------------------


def _hint(cls: type, field: str) -> object:
    return typing.get_type_hints(cls, include_extras=True)[field]


def test_druid_conn_fields_wire_expected_specs():
    assert _hint(DruidDestConn, "overlord_url") == UrlSpec
    assert _hint(DruidDestConn, "staging") == CloudLocation
    assert _hint(DruidDestConn, "credentials") == CredentialsSpec
    assert _hint(DruidDestConn, "conn_cfg") == CfgSpec | None


def test_druid_dest_fields_wire_expected_specs():
    assert _hint(DruidDest, "tables") == DruidTableNamesSpec
    assert _hint(DruidDest, "if_table_exists") == IfTableExistsSpec
    assert _hint(DruidDest, "time_column") == str | None
    assert _hint(DruidDest, "segment_granularity") == SegmentGranularitySpec
    assert _hint(DruidDest, "max_concurrent_subtasks") is int
    assert _hint(DruidDest, "max_rows_per_segment") is int
    assert _hint(DruidDest, "max_rows_in_memory") is int
    assert _hint(DruidDest, "http_timeout_sec") is int
    assert _hint(DruidDest, "dest_cfg") == DestCfgSpec | None


# ---------------------------------------------------------------------------
# dest_cfg literal-key validation
# ---------------------------------------------------------------------------


def test_druid_dest_rejects_unknown_dest_cfg_key():
    with pytest.raises(ConnInitException) as ei:
        DruidDest(
            tables=["my_datasource"],
            dest_cfg={"bogus.key": 1},
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_2


def test_druid_dest_accepts_known_dest_cfg_keys():
    dest = DruidDest(
        tables=["my_datasource"],
        dest_cfg={
            "druid.logging_level": 20,
            "druid.poll_interval_seconds": 5.0,
            "druid.retry_attempts": 4,
            "druid.retry_backoff_seconds": 1.5,
        },
    )
    assert dest.dest_cfg == {
        "druid.logging_level": 20,
        "druid.poll_interval_seconds": 5.0,
        "druid.retry_attempts": 4,
        "druid.retry_backoff_seconds": 1.5,
    }


def test_druid_conn_rejects_missing_overlord_url():
    with pytest.raises(ConnInitException) as ei:
        DruidDestConn(  # type: ignore[call-arg]
            staging=_staging(),
            credentials=UserPassword(
                user=Secret("u"),
                password=Secret("p"),
            ),
        )
    assert ei.value.api_error is DruidErrorCode.DRUID_1

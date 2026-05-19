
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.s3.error import S3ValidateException
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.s3 import S3Conn, S3Dest, S3Src
from tabsdatak.conn.s3.error import S3ErrorCode


pytestmark = pytest.mark.s3


def _conn():
    return S3Conn(
        bucket="my-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        conn_cfg={"s3.timeout": 30},
    )


def test_s3_src_full_round_trip():
    src = S3Src(
        conn=_conn(),
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"tabsdata.file.chunkSize": 1024},
    )
    loaded = S3Src.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.bucket == "my-bucket"
    assert loaded.conn.region == "us-east-1"
    assert loaded.conn.conn_cfg == {"s3.timeout": 30}
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"tabsdata.file.chunkSize": 1024}


def test_s3_dest_full_round_trip():
    dest = S3Dest(
        conn=_conn(),
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunkSize": 2048},
    )
    loaded = S3Dest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.bucket == "my-bucket"
    assert loaded.conn.conn_cfg == {"s3.timeout": 30}
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunkSize": 2048}


def test_s3_src_round_trip_with_named_conn():
    src = S3Src(
        conn=NamedConn(name="prod_s3"),
        paths=["raw/users.csv"],
        format=SrcFileFormat.CSV,
    )
    loaded = S3Src.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_s3"


def test_s3_dest_round_trip_with_named_conn():
    dest = S3Dest(
        conn=NamedConn(name="prod_s3"),
        paths=["out/users.csv"],
        format=DestFileFormat.CSV,
    )
    loaded = S3Dest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_s3"


def test_s3_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(S3ValidateException) as ei:
        S3Src(
            conn=_conn(),
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is S3ErrorCode.S3_3

    with pytest.raises(S3ValidateException) as ei:
        S3Dest(
            conn=_conn(),
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is S3ErrorCode.S3_5


# ---------------------------------------------------------------------------
# S3Conn.full_path
# ---------------------------------------------------------------------------


def _conn_with_base(base_path):
    return S3Conn(
        bucket="my-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        base_path=base_path,
    )


def test_s3_conn_default_base_path_is_empty():
    c = _conn_with_base("")
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_s3_conn_full_path_treats_absolute_path_as_relative():
    c = _conn_with_base("/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_s3_conn_full_path_strips_trailing_slashes():
    c = _conn_with_base("/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"

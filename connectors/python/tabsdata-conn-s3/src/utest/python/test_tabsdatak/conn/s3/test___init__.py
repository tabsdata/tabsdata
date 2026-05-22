
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
from tabsdatak.conn.s3 import S3Dest, S3DestConn, S3Src, S3SrcConn
from tabsdatak.conn.s3.error import S3ErrorCode


pytestmark = pytest.mark.s3


def _src_conn() -> S3SrcConn:
    return S3SrcConn(
        bucket="my-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        conn_cfg={"s3.timeout": 30},
    )


def _dest_conn() -> S3DestConn:
    return S3DestConn(
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
        conn=_src_conn(),
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
        conn=_dest_conn(),
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
            conn=_src_conn(),
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is S3ErrorCode.S3_3

    with pytest.raises(S3ValidateException) as ei:
        S3Dest(
            conn=_dest_conn(),
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is S3ErrorCode.S3_5


# ---------------------------------------------------------------------------
# S3Conn.full_path
# ---------------------------------------------------------------------------


@pytest.fixture(params=[S3SrcConn, S3DestConn])
def conn_cls(request):
    return request.param


def _conn_with_base(cls, base_path):
    return cls(
        bucket="my-bucket",
        region="us-east-1",
        credentials=AwsAccessSecretKey(
            access_key_id=Secret.lit("AKIA..."),
            secret_access_key=Secret.lit("secret"),
        ),
        base_path=base_path,
    )


def test_s3_conn_default_base_path_is_empty(conn_cls):
    c = _conn_with_base(conn_cls, "")
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_s3_conn_full_path_treats_absolute_path_as_relative(conn_cls):
    c = _conn_with_base(conn_cls, "/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_s3_conn_full_path_strips_trailing_slashes(conn_cls):
    c = _conn_with_base(conn_cls, "/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"


def test_s3_src_dest_conn_have_distinct_kinds():
    assert S3SrcConn.__kind__ != S3DestConn.__kind__


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation
# ---------------------------------------------------------------------------


def test_s3_conn_rejects_missing_bucket(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(  # type: ignore[call-arg]
            region="us-east-1",
            credentials=AwsAccessSecretKey(
                access_key_id=Secret.lit("AKIA..."),
                secret_access_key=Secret.lit("secret"),
            ),
        )
    assert ei.value.api_error is S3ErrorCode.S3_1


def test_s3_conn_rejects_missing_credentials(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(bucket="my-bucket", region="us-east-1")  # type: ignore[call-arg]
    assert ei.value.api_error is S3ErrorCode.S3_1


def test_s3_src_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        S3Src(  # type: ignore[call-arg]
            conn=_src_conn(),
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is S3ErrorCode.S3_2


def test_s3_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        S3Dest(  # type: ignore[call-arg]
            conn=_dest_conn(),
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is S3ErrorCode.S3_4

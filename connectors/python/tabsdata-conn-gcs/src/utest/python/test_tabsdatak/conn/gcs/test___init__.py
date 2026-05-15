
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.gcs.error import GcsValidateException
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.gcs import GCSConn, GCSDest, GCSSrc
from tabsdatak.conn.gcs.error import GcsErrorCode


pytestmark = pytest.mark.gcs


def _conn():
    return GCSConn(
        bucket="my-bucket",
        credentials=GCPJson(json=Secret("gcp-sa#KEY")),
        project="my-project",
        conn_cfg={"gcs.timeout": 30},
    )


def test_gcs_src_full_round_trip():
    src = GCSSrc(
        conn=_conn(),
        path=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"chunkSize": 1024},
    )
    loaded = GCSSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.bucket == "my-bucket"
    assert loaded.conn.project.value() == "my-project"
    assert loaded.conn.conn_cfg == {"gcs.timeout": 30}
    assert loaded.path == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_gcs_dest_full_round_trip():
    dest = GCSDest(
        conn=_conn(),
        path=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"chunkSize": 2048},
    )
    loaded = GCSDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.bucket == "my-bucket"
    assert loaded.conn.conn_cfg == {"gcs.timeout": 30}
    assert loaded.path == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_gcs_src_round_trip_with_named_conn():
    src = GCSSrc(
        conn=NamedConn(name="prod_gcs"),
        path=["raw/users.csv"],
        format=SrcFileFormat.CSV,
    )
    loaded = GCSSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_gcs"


def test_gcs_dest_round_trip_with_named_conn():
    dest = GCSDest(
        conn=NamedConn(name="prod_gcs"),
        path=["out/users.csv"],
        format=DestFileFormat.CSV,
    )
    loaded = GCSDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_gcs"


def test_gcs_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(GcsValidateException) as ei:
        GCSSrc(
            conn=_conn(),
            path=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is GcsErrorCode.GCS_3

    with pytest.raises(GcsValidateException) as ei:
        GCSDest(
            conn=_conn(),
            path=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is GcsErrorCode.GCS_5


# ---------------------------------------------------------------------------
# GCSConn.full_path
# ---------------------------------------------------------------------------


def _conn_with_base(base_path):
    return GCSConn(
        bucket="my-bucket",
        credentials=GCPJson(json=Secret("gcp-sa#KEY")),
        base_path=base_path,
    )


def test_gcs_conn_default_base_path_is_empty():
    c = _conn_with_base("")
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_gcs_conn_full_path_treats_absolute_path_as_relative():
    c = _conn_with_base("/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_gcs_conn_full_path_strips_trailing_slashes():
    c = _conn_with_base("/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"

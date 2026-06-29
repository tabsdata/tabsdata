
from datetime import datetime, timezone

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.gcs.error import GcsValidateException
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn, GCSDest, GCSSrc
from tabsdatak.conn.gcs.error import GcsErrorCode


pytestmark = pytest.mark.gcs


def test_gcs_src_full_round_trip():
    src = GCSSrc(
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"tabsdata.file.chunkSize": 1024},
    )
    loaded = GCSSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"tabsdata.file.chunkSize": 1024}


def test_gcs_dest_full_round_trip():
    dest = GCSDest(
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunkSize": 2048},
    )
    loaded = GCSDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunkSize": 2048}


def test_gcs_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(GcsValidateException) as ei:
        GCSSrc(
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is GcsErrorCode.GCS_3

    with pytest.raises(GcsValidateException) as ei:
        GCSDest(
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is GcsErrorCode.GCS_5


# ---------------------------------------------------------------------------
# GCSConn.full_path
# ---------------------------------------------------------------------------


@pytest.fixture(params=[GCSSrcConn, GCSDestConn])
def conn_cls(request):
    return request.param


def _conn_with_base(cls, base_path):
    return cls(
        bucket="my-bucket",
        credentials=GCPJson(json=Secret()),
        base_path=base_path,
    )


def test_gcs_conn_default_base_path_is_empty(conn_cls):
    c = _conn_with_base(conn_cls, "")
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_gcs_conn_full_path_treats_absolute_path_as_relative(conn_cls):
    c = _conn_with_base(conn_cls, "/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_gcs_conn_full_path_strips_trailing_slashes(conn_cls):
    c = _conn_with_base(conn_cls, "/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"


def test_gcs_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert GCSSrcConn is not GCSDestConn
    assert GCSSrcConn.__kind__ == GCSDestConn.__kind__ == "connectionDef"


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation
# ---------------------------------------------------------------------------


def test_gcs_conn_rejects_missing_bucket(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(credentials=GCPJson(json=Secret()))  # type: ignore[call-arg]
    assert ei.value.api_error is GcsErrorCode.GCS_1


def test_gcs_conn_rejects_missing_credentials(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(bucket="my-bucket")  # type: ignore[call-arg]
    assert ei.value.api_error is GcsErrorCode.GCS_1


def test_gcs_src_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        GCSSrc(  # type: ignore[call-arg]
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is GcsErrorCode.GCS_2


def test_gcs_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        GCSDest(  # type: ignore[call-arg]
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is GcsErrorCode.GCS_4

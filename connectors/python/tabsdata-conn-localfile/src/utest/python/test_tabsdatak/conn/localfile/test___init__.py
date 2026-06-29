
from datetime import datetime, timezone

import pytest

from tabsdatak.conn.localfile.error import LocalFileValidateException
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.localfile import LocalFileDest, LocalFileDestConn, LocalFileSrc, LocalFileSrcConn
from tabsdatak.conn.localfile.error import LocalFileErrorCode


pytestmark = pytest.mark.localfile


@pytest.fixture(params=[LocalFileSrcConn, LocalFileDestConn])
def conn_cls(request):
    return request.param


def test_localfile_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert LocalFileSrcConn is not LocalFileDestConn
    assert LocalFileSrcConn.__kind__ == LocalFileDestConn.__kind__ == "connectionDef"


def test_localfile_src_full_round_trip():
    src = LocalFileSrc(
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"tabsdata.file.chunkSize": 1024},
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"tabsdata.file.chunkSize": 1024}


def test_localfile_dest_full_round_trip():
    dest = LocalFileDest(
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunkSize": 2048},
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunkSize": 2048}


def test_localfile_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileSrc(
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_3

    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileDest(
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_5


# ---------------------------------------------------------------------------
# LocalFile{Src,Dest}Conn.full_path
# ---------------------------------------------------------------------------


def test_localfile_conn_default_base_path_is_empty(conn_cls):
    c = conn_cls()
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_localfile_conn_full_path_treats_absolute_path_as_relative(conn_cls):
    c = conn_cls(base_path="/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_localfile_conn_full_path_strips_trailing_slashes(conn_cls):
    c = conn_cls(base_path="/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation
# ---------------------------------------------------------------------------


def test_localfile_conn_rejects_bad_base_path_type(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(base_path=42)  # type: ignore[arg-type]
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_1


def test_localfile_src_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        LocalFileSrc(  # type: ignore[call-arg]
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_2


def test_localfile_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        LocalFileDest(  # type: ignore[call-arg]
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_4

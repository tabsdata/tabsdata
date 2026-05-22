
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn
from tabsdatak.conn.localfile.error import LocalFileValidateException
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.localfile import LocalFileDest, LocalFileDestConn, LocalFileSrc, LocalFileSrcConn
from tabsdatak.conn.localfile.error import LocalFileErrorCode


pytestmark = pytest.mark.localfile


def _src_conn():
    return LocalFileSrcConn(
        base_path="/data/",
        conn_cfg={"localfile.timeout": 30},
    )


def _dest_conn():
    return LocalFileDestConn(
        base_path="/data/",
        conn_cfg={"localfile.timeout": 30},
    )


@pytest.fixture(params=[LocalFileSrcConn, LocalFileDestConn])
def conn_cls(request):
    return request.param


def test_localfile_src_dest_conn_have_distinct_kinds():
    assert LocalFileSrcConn.__kind__ != LocalFileDestConn.__kind__


def test_localfile_src_full_round_trip():
    src = LocalFileSrc(
        conn=_src_conn(),
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"tabsdata.file.chunkSize": 1024},
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.base_path == "/data/"
    assert loaded.conn.conn_cfg == {"localfile.timeout": 30}
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
        conn=_dest_conn(),
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunkSize": 2048},
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.base_path == "/data/"
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunkSize": 2048}


def test_localfile_src_round_trip_with_named_conn():
    src = LocalFileSrc(
        conn=NamedConn(name="prod_localfile"),
        paths=["raw/users.csv"],
        format=SrcFileFormat.CSV,
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_localfile"


def test_localfile_dest_round_trip_with_named_conn():
    dest = LocalFileDest(
        conn=NamedConn(name="prod_localfile"),
        paths=["out/users.csv"],
        format=DestFileFormat.CSV,
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_localfile"


def test_localfile_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileSrc(
            conn=_src_conn(),
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_3

    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileDest(
            conn=_dest_conn(),
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
            conn=_src_conn(),
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_2


def test_localfile_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        LocalFileDest(  # type: ignore[call-arg]
            conn=_dest_conn(),
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_4

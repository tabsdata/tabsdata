
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn
from tabsdatak.conn.localfile.error import LocalFileValidateException
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.localfile import LocalFileConn, LocalFileDest, LocalFileSrc
from tabsdatak.conn.localfile.error import LocalFileErrorCode


pytestmark = pytest.mark.localfile


def _conn():
    return LocalFileConn(
        base_path="/data/",
        conn_cfg={"localfile.timeout": 30},
    )


def test_localfile_src_full_round_trip():
    src = LocalFileSrc(
        conn=_conn(),
        path=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"chunkSize": 1024},
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.base_path == "/data/"
    assert loaded.conn.conn_cfg == {"localfile.timeout": 30}
    assert loaded.path == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"chunkSize": 1024}


def test_localfile_dest_full_round_trip():
    dest = LocalFileDest(
        conn=_conn(),
        path=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"chunkSize": 2048},
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.base_path == "/data/"
    assert loaded.path == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"chunkSize": 2048}


def test_localfile_src_round_trip_with_named_conn():
    src = LocalFileSrc(
        conn=NamedConn(name="prod_localfile"),
        path=["raw/users.csv"],
        format=SrcFileFormat.CSV,
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_localfile"


def test_localfile_dest_round_trip_with_named_conn():
    dest = LocalFileDest(
        conn=NamedConn(name="prod_localfile"),
        path=["out/users.csv"],
        format=DestFileFormat.CSV,
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_localfile"


def test_localfile_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileSrc(
            conn=_conn(),
            path=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_3

    with pytest.raises(LocalFileValidateException) as ei:
        LocalFileDest(
            conn=_conn(),
            path=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is LocalFileErrorCode.LOCALFILE_5


# ---------------------------------------------------------------------------
# LocalFileConn.full_path
# ---------------------------------------------------------------------------


def test_localfile_conn_default_base_path_is_empty():
    c = LocalFileConn()
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_localfile_conn_full_path_treats_absolute_path_as_relative():
    c = LocalFileConn(base_path="/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_localfile_conn_full_path_strips_trailing_slashes():
    c = LocalFileConn(base_path="/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"

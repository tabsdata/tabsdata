#
# Copyright 2026 Tabsdata Inc.
#


from datetime import datetime, timezone

import pytest

from tabsdatak.conn.localfile.error import LocalFileValidateException
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
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
        src_cfg={"tabsdata.file.chunk_size": 1024},
    )
    loaded = LocalFileSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.src_cfg == {"tabsdata.file.chunk_size": 1024}


def test_localfile_dest_full_round_trip():
    dest = LocalFileDest(
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunk_size": 2048},
    )
    loaded = LocalFileDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunk_size": 2048}


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
# LocalFile{Src,Dest}Conn.base_path
# ---------------------------------------------------------------------------


def test_localfile_conn_default_base_path_is_root(conn_cls):
    c = conn_cls()
    assert c.base_path == "/"


def test_localfile_conn_accepts_absolute_base_path(conn_cls):
    c = conn_cls(base_path="/data/sub")
    assert c.base_path == "/data/sub"


@pytest.mark.parametrize("bad", ["", "data", "data/sub", "/data/", "/data//sub"])
def test_localfile_conn_rejects_non_absolute_or_trailing_slash(conn_cls, bad):
    # FullPathSpec requires a leading "/" and forbids a trailing one;
    # a bare string that fails the pattern is rejected by the
    # ValueConstraints rules at construction time.
    with pytest.raises(Exception):
        conn_cls(base_path=bad)


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

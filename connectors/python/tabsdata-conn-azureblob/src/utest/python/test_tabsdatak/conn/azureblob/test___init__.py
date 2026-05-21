
from datetime import datetime, timezone

import pytest

from tabsdatak.api import NamedConn, Secret
from tabsdatak.conn.azureblob.error import AzureBlobValidateException
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    FileInfo,
    SrcFileFormat,
)
from tabsdatak.conn.azureblob import AzureBlobConn, AzureBlobDest, AzureBlobSrc
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode


pytestmark = pytest.mark.azureblob


def _conn():
    return AzureBlobConn(
        container=Secret(),
        credentials=AzureAccountKey(
            account_name=Secret(),
            account_key=Secret(),
        ),
        conn_cfg={"azure.timeout": 30},
    )


def test_azureblob_src_full_round_trip():
    src = AzureBlobSrc(
        conn=_conn(),
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        file_info=FileInfo.FULLPATH,
        src_cfg={"tabsdata.file.chunkSize": 1024},
    )
    loaded = AzureBlobSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.conn.conn_cfg == {"azure.timeout": 30}
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.file_info is FileInfo.FULLPATH
    assert loaded.src_cfg == {"tabsdata.file.chunkSize": 1024}


def test_azureblob_dest_full_round_trip():
    dest = AzureBlobDest(
        conn=_conn(),
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunkSize": 2048},
    )
    loaded = AzureBlobDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.conn.conn_cfg == {"azure.timeout": 30}
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunkSize": 2048}


def test_azureblob_src_round_trip_with_named_conn():
    src = AzureBlobSrc(
        conn=NamedConn(name="prod_azure"),
        paths=["raw/users.csv"],
        format=SrcFileFormat.CSV,
    )
    loaded = AzureBlobSrc.from_json(src.to_json())
    assert loaded == src
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_azure"


def test_azureblob_dest_round_trip_with_named_conn():
    dest = AzureBlobDest(
        conn=NamedConn(name="prod_azure"),
        paths=["out/users.csv"],
        format=DestFileFormat.CSV,
    )
    loaded = AzureBlobDest.from_json(dest.to_json())
    assert loaded == dest
    assert isinstance(loaded.conn, NamedConn)
    assert loaded.conn.name == "prod_azure"


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation
# ---------------------------------------------------------------------------


def test_azureblob_conn_rejects_missing_container():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobConn(  # type: ignore[call-arg]
            credentials=AzureAccountKey(
                account_name=Secret(),
                account_key=Secret(),
            ),
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_1


def test_azureblob_conn_rejects_missing_credentials():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobConn(container=Secret())  # type: ignore[call-arg]
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_1


def test_azureblob_src_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobSrc(  # type: ignore[call-arg]
            conn=_conn(),
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_2


def test_azureblob_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobDest(  # type: ignore[call-arg]
            conn=_conn(),
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_4


def test_azureblob_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobSrc(
            conn=_conn(),
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_3

    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobDest(
            conn=_conn(),
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_5


# ---------------------------------------------------------------------------
# AzureBlobConn.full_path
# ---------------------------------------------------------------------------


def _conn_with_base(base_path):
    return AzureBlobConn(
        container=Secret(),
        credentials=AzureAccountKey(
            account_name=Secret(),
            account_key=Secret(),
        ),
        base_path=base_path,
    )


def test_azureblob_conn_default_base_path_is_empty():
    c = _conn_with_base("")
    assert c.base_path == ""
    assert c.full_path("foo.csv") == "foo.csv"


def test_azureblob_conn_full_path_treats_absolute_path_as_relative():
    c = _conn_with_base("/data/")
    assert c.full_path("/foo.csv") == "data/foo.csv"
    assert c.full_path("foo.csv") == "data/foo.csv"


def test_azureblob_conn_full_path_strips_trailing_slashes():
    c = _conn_with_base("/data")
    assert c.full_path("sub/foo.csv/") == "data/sub/foo.csv"
    assert c.full_path("/") == "data"

#
# Copyright 2026 Tabsdata Inc.
#


from datetime import datetime, timezone

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.azureblob.error import AzureBlobValidateException
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import (
    DestFileFormat,
    SrcFileFormat,
)
from tabsdatak.conn.azureblob import (
    AzureBlobDest,
    AzureBlobDestConn,
    AzureBlobSrc,
    AzureBlobSrcConn,
)
from tabsdatak.conn.azureblob.error import AzureBlobErrorCode


pytestmark = pytest.mark.azureblob


def test_azureblob_src_full_round_trip():
    src = AzureBlobSrc(
        paths=["raw/users.csv", "raw/orders/*.csv"],
        format=SrcFileFormat.CSV,
        format_cfg={SrcFileFormat.CSV: {"separator": ";", "has_header": True}},
        initial_last_modified=datetime(2026, 1, 1, tzinfo=timezone.utc),
        src_cfg={"tabsdata.file.chunk_size": 1024},
    )
    loaded = AzureBlobSrc.from_json(src.to_json())
    assert loaded == src
    assert loaded.paths == ["raw/users.csv", "raw/orders/*.csv"]
    assert loaded.format is SrcFileFormat.CSV
    assert loaded.format_cfg == {
        SrcFileFormat.CSV: {"separator": ";", "has_header": True}
    }
    assert loaded.initial_last_modified == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert loaded.src_cfg == {"tabsdata.file.chunk_size": 1024}


def test_azureblob_dest_full_round_trip():
    dest = AzureBlobDest(
        paths=["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"],
        format=DestFileFormat.CSV,
        format_cfg={DestFileFormat.CSV: {"include_header": True, "separator": ","}},
        dest_cfg={"tabsdata.file.chunk_size": 2048},
    )
    loaded = AzureBlobDest.from_json(dest.to_json())
    assert loaded == dest
    assert loaded.paths == ["${EXEC_PLAN_ID}/users.csv", "${TRX_ID}/orders.csv"]
    assert loaded.format is DestFileFormat.CSV
    assert loaded.format_cfg == {
        DestFileFormat.CSV: {"include_header": True, "separator": ","}
    }
    assert loaded.dest_cfg == {"tabsdata.file.chunk_size": 2048}


# ---------------------------------------------------------------------------
# Conn / Src / Dest init validation
# ---------------------------------------------------------------------------


@pytest.fixture(params=[AzureBlobSrcConn, AzureBlobDestConn])
def conn_cls(request):
    return request.param


def test_azureblob_conn_rejects_missing_container(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(  # type: ignore[call-arg]
            credentials=AzureAccountKey(
                account_name=Secret(),
                account_key=Secret(),
            ),
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_1


def test_azureblob_conn_rejects_missing_credentials(conn_cls):
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        conn_cls(container=Secret())  # type: ignore[call-arg]
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_1


def test_azureblob_src_dest_conn_are_distinct_classes():
    # Source and destination use distinct connection classes (told apart
    # on the wire by the envelope ``type``), though both now share the
    # uniform ``connectionDef`` kind.
    assert AzureBlobSrcConn is not AzureBlobDestConn
    assert AzureBlobSrcConn.__kind__ == AzureBlobDestConn.__kind__ == "connectionDef"


def test_azureblob_src_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobSrc(  # type: ignore[call-arg]
            format=SrcFileFormat.CSV,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_2


def test_azureblob_dest_rejects_missing_paths():
    from tabsdatak.conn.common.error import ConnInitException

    with pytest.raises(ConnInitException) as ei:
        AzureBlobDest(  # type: ignore[call-arg]
            format=DestFileFormat.CSV,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_4


def test_azureblob_validate_rejects_cross_side_format_cfg_keys():
    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobSrc(
            paths=["data.csv"],
            format=SrcFileFormat.CSV,
            format_cfg={SrcFileFormat.CSV: {"include_header": True}},
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_3

    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobDest(
            paths=["out.csv"],
            format=DestFileFormat.CSV,
            format_cfg={DestFileFormat.CSV: {"has_header": True}},
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_5


# ---------------------------------------------------------------------------
# AzureBlobConn.base_path
# ---------------------------------------------------------------------------


def _conn_with_base(cls, base_path):
    return cls(
        container=Secret(),
        credentials=AzureAccountKey(
            account_name=Secret(),
            account_key=Secret(),
        ),
        base_path=base_path,
    )


def test_azureblob_conn_default_base_path_is_root(conn_cls):
    c = conn_cls(
        container=Secret(),
        credentials=AzureAccountKey(
            account_name=Secret(),
            account_key=Secret(),
        ),
    )
    assert c.base_path == "/"


def test_azureblob_conn_accepts_absolute_base_path(conn_cls):
    c = _conn_with_base(conn_cls, "/data/sub")
    assert c.base_path == "/data/sub"


@pytest.mark.parametrize("bad", ["data", "data/sub", "/data/", "/data//sub"])
def test_azureblob_conn_rejects_non_absolute_or_trailing_slash_base_path(conn_cls, bad):
    # A bare string that fails the FullPathSpec pattern is rejected by the
    # ValueConstraints rules at construction time.
    with pytest.raises(Exception):
        _conn_with_base(conn_cls, bad)


# ---------------------------------------------------------------------------
# AzureBlobSrc with SrcFileFormat.EBCDIC
# ---------------------------------------------------------------------------


def _copybook(record: str) -> str:
    """A minimal copybook source. `copybook` carries the COBOL itself, not a
    path to it, so fixtures have to be real (if tiny) layouts."""
    return f"      01  {record}.\n          05  FIELD PIC X(4).\n"


def _ebcdic_src(paths=("extracts/jan.dat",), **cfg):
    cfg.setdefault("copybook", [_copybook("CUSTOMER")] * len(paths))
    return AzureBlobSrc(
        paths=list(paths),
        format=SrcFileFormat.EBCDIC,
        format_cfg={SrcFileFormat.EBCDIC: cfg},
    )


def test_azureblob_ebcdic_src_full_round_trip():
    copybooks = [_copybook("JAN"), _copybook("FEB")]
    src = _ebcdic_src(
        paths=("extracts/jan.dat", "extracts/feb.dat"),
        copybook=copybooks,
        codepage="cp500",
    )
    loaded = AzureBlobSrc.from_json(src.to_json())

    assert loaded == src
    assert loaded.format is SrcFileFormat.EBCDIC
    # The copybook survives the round trip whole -- its newlines and leading
    # columns are load-bearing, since COBOL code starts at column 8.
    assert loaded.format_cfg[SrcFileFormat.EBCDIC]["copybook"] == copybooks
    assert loaded.format_cfg[SrcFileFormat.EBCDIC]["codepage"] == "cp500"


def test_azureblob_ebcdic_src_defaults_the_codepage():
    src = _ebcdic_src()
    assert "codepage" not in src.format_cfg[SrcFileFormat.EBCDIC]


def test_azureblob_ebcdic_src_needs_a_copybook():
    # The data carries no schema at all, so this can never be defaulted.
    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobSrc(
            paths=["extracts/jan.dat"],
            format=SrcFileFormat.EBCDIC,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_3
    assert ei.value.cause.api_error is FileCommonErrorCode.FILE_COMMON_31


def test_azureblob_ebcdic_src_needs_one_copybook_per_path():
    with pytest.raises(AzureBlobValidateException) as ei:
        _ebcdic_src(paths=("jan.dat", "feb.dat"), copybook=[_copybook("CUSTOMER")])
    assert ei.value.cause.api_error is FileCommonErrorCode.FILE_COMMON_32


def test_azureblob_ebcdic_src_rejects_a_wildcard_path():
    # A wildcard is an unknown number of files, which no fixed list of
    # copybooks can pair with.
    with pytest.raises(AzureBlobValidateException) as ei:
        _ebcdic_src(paths=("extracts/*.dat",))
    assert ei.value.cause.api_error is FileCommonErrorCode.FILE_COMMON_33


def test_azureblob_ebcdic_src_rejects_an_unsupported_codepage():
    with pytest.raises(AzureBlobValidateException) as ei:
        _ebcdic_src(codepage="cp1047")
    assert ei.value.cause.api_error is FileCommonErrorCode.FILE_COMMON_34


def test_azureblob_ebcdic_src_rejects_a_key_it_does_not_define():
    # The binary has more reader knobs; none is reachable from format_cfg.
    with pytest.raises(AzureBlobValidateException) as ei:
        _ebcdic_src(hfp=False)
    assert ei.value.cause.api_error is FileCommonErrorCode.FILE_COMMON_11


def test_azureblob_ebcdic_is_read_only():
    # There is no DestFileFormat.EBCDIC: a copybook describes a layout to
    # read, and the pipeline never writes one.
    assert not hasattr(DestFileFormat, "EBCDIC")


# --- worksheet fragments --------------------------------------------------
#
# A `#<sheet>` fragment is checked when the descriptor is built, so a typo
# fails registration rather than the first run.


def test_azureblob_src_accepts_a_worksheet_fragment():
    src = AzureBlobSrc(paths=["book.xlsx#2"])
    assert src.paths == ["book.xlsx#2"]


@pytest.mark.parametrize("path", ["book.xlsx#abc", "book.xlsx#0", "rows.csv#2"])
def test_azureblob_src_rejects_a_bad_worksheet_fragment_at_declaration_time(path):
    with pytest.raises(AzureBlobValidateException) as ei:
        AzureBlobSrc(
            paths=[path],
            format=SrcFileFormat.EXCEL if "xlsx" in path else SrcFileFormat.CSV,
        )
    assert ei.value.api_error is AzureBlobErrorCode.AZURE_BLOB_3

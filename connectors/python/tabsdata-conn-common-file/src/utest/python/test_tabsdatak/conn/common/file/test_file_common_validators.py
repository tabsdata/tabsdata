
import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat


# --- src path ------------------------------------------------------------


def test_src_path_accepts_simple_name():
    assert _validation.validate_src_path("data.csv") == "data.csv"


def test_src_path_accepts_glob_in_basename():
    assert _validation.validate_src_path("dir/*.csv") == "dir/*.csv"


def test_src_path_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_src_path("")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_1


def test_src_path_rejects_backslash():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_src_path("dir\\file.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_3


def test_src_path_rejects_token():
    # ${...} substitution tokens are destination-only.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_src_path("dir/${EXEC_PLAN_ID}.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_4


def test_src_path_rejects_glob_outside_basename():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_src_path("dir/*/file.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_5


# --- dest path ----------------------------------------------------------


def test_dest_path_accepts_simple_name():
    assert _validation.validate_dest_path("out.csv") == "out.csv"


def test_dest_path_accepts_known_token():
    assert _validation.validate_dest_path("out/${EXEC_PLAN_ID}.csv") == "out/${EXEC_PLAN_ID}.csv"


def test_dest_path_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_dest_path("")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_6


def test_dest_path_rejects_glob():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_dest_path("out/*.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_9


def test_dest_path_rejects_unknown_token():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_dest_path("out/${UNKNOWN_TOKEN}.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_10


# --- format_cfg ---------------------------------------------------------


def test_format_cfg_accepts_none():
    assert _validation.validate_format_cfg(None, side="src") is None


def test_format_cfg_accepts_valid_csv_src_keys():
    cfg = {SrcFileFormat.CSV: {"separator": ",", "encoding": "utf-8"}}
    assert _validation.validate_format_cfg(cfg, side="src") == cfg


def test_format_cfg_accepts_valid_csv_dest_keys():
    cfg = {DestFileFormat.CSV: {"separator": ",", "include_header": True}}
    assert _validation.validate_format_cfg(cfg, side="dest") == cfg


def test_format_cfg_rejects_dest_only_key_on_src():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: {"include_header": True}}, side="src"
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_11


def test_format_cfg_rejects_src_only_key_on_dest():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {DestFileFormat.CSV: {"has_header": True}}, side="dest"
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_11


def test_format_cfg_rejects_non_dict_value():
    # Non-dict cfg -> FILECOMMON_4 with format + got_type rendered
    # into the primary message (no chained ValueError needed).
    with pytest.raises(ConnException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: "not a dict"}, side="src"  # type: ignore[dict-item]
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_12
    msg = str(ei.value)
    assert "format_cfg[CSV]" in msg
    assert "got str" in msg


def test_format_cfg_rejects_unknown_key_for_non_csv():
    # JSON / AVRO / PARQUET accept no keys today.
    with pytest.raises(ConnInitException):
        _validation.validate_format_cfg(
            {SrcFileFormat.JSON: {"separator": ","}}, side="src"
        )


def test_unknown_key_message_inlines_allowed_keys_for_csv_src():
    # The recovery path -- "here are the keys you *can* use" --
    # must be in the primary error message, not buried as a cause.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: {"include_header": True}}, side="src"
        )
    msg = str(ei.value)
    # Diagnosis (what's wrong):
    assert "format_cfg[CSV]" in msg
    assert "['include_header']" in msg
    # Recovery (what's allowed) -- a sample of the CSV-src key set:
    assert "encoding" in msg
    assert "has_header" in msg
    assert "separator" in msg
    # The dest-only key is *not* listed as allowed on the src side.
    assert "include_header'" not in msg.split("allowed for", 1)[1]
    # Side is named explicitly so the LLM can tell why the key set
    # differs from the dest variant.
    assert "(src)" in msg


def test_unknown_key_message_inlines_allowed_keys_for_csv_dest():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {DestFileFormat.CSV: {"has_header": True}}, side="dest"
        )
    msg = str(ei.value)
    assert "format_cfg[CSV]" in msg
    assert "(dest)" in msg
    # A sample of the CSV-dest key set:
    assert "include_header" in msg
    assert "datetime_format" in msg
    assert "quote_style" in msg


def test_unknown_key_message_inlines_none_for_keyless_format():
    # JSON accepts no format_cfg keys today; the message should
    # surface that as the literal "(none)" so an LLM doesn't guess.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.JSON: {"separator": ","}}, side="src"
        )
    msg = str(ei.value)
    assert "format_cfg[JSON]" in msg
    assert "(none)" in msg


def test_unknown_key_extras_carried_for_programmatic_access():
    # Same data the message renders is also available structured
    # on `e.extra` for callers that want to react programmatically
    # rather than parse the string.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: {"include_header": True, "datetime_format": "x"}},
            side="src",
        )
    extras = ei.value.extra
    assert extras["format"] == "CSV"
    assert extras["side"] == "src"
    assert extras["unknown"] == ["datetime_format", "include_header"]
    assert "encoding" in extras["allowed"]


# --- src/dest path NUL byte + dest backslash + bad side ----------------


def test_src_path_rejects_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_src_path("dir/has\x00null.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_2


def test_dest_path_rejects_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_dest_path("dir/has\x00null.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_7


def test_dest_path_rejects_backslash():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_dest_path(r"dir\file.csv")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_8


def test_format_cfg_rejects_bad_side():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: {}}, side="other",
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_13

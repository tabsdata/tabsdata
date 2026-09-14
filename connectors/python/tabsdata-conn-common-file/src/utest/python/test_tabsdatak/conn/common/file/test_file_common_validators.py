#
# Copyright 2026 Tabsdata Inc.
#


import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.file import _validation
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.error import ConstraintError
from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat


# --- src path ------------------------------------------------------------


def test_src_path_accepts_simple_name():
    assert _validation.SRC_PATH_VALIDATION.validate("data.csv") == "data.csv"


def test_src_path_accepts_glob_in_basename():
    assert _validation.SRC_PATH_VALIDATION.validate("dir/*.csv") == "dir/*.csv"


def test_src_path_rejects_empty():
    with pytest.raises(ConstraintError) as ei:
        _validation.SRC_PATH_VALIDATION.validate("")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_1


def test_src_path_accepts_backslash_as_separator():
    # A backslash is a valid path separator (normalized per-OS at use time).
    assert _validation.SRC_PATH_VALIDATION.validate("dir\\file.csv") == "dir\\file.csv"


def test_src_path_rejects_dot_segments():
    for value in ("..", "dir/../file.csv", "dir\\..\\file.csv", "./file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.SRC_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_3


def test_src_path_rejects_token():
    # ${...} substitution tokens are destination-only.
    with pytest.raises(ConstraintError) as ei:
        _validation.SRC_PATH_VALIDATION.validate("dir/${EXEC_PLAN_ID}.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_4


def test_src_path_rejects_glob_outside_basename():
    with pytest.raises(ConstraintError) as ei:
        _validation.SRC_PATH_VALIDATION.validate("dir/*/file.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_5


def test_src_path_rejects_leading_slash():
    # A source path is relative to the connection's base_path; either
    # separator counts as an (illegal) leading anchor.
    for value in ("/dir/file.csv", "\\dir\\file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.SRC_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_22


def test_src_path_rejects_trailing_slash():
    for value in ("dir/", "dir\\"):
        with pytest.raises(ConstraintError) as ei:
            _validation.SRC_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_23


def test_src_path_rejects_double_slash():
    for value in ("dir//file.csv", "dir\\\\file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.SRC_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_24


# --- dest path ----------------------------------------------------------


def test_dest_path_accepts_simple_name():
    assert _validation.DEST_PATH_VALIDATION.validate("out.csv") == "out.csv"


def test_dest_path_accepts_known_token():
    assert (
        _validation.DEST_PATH_VALIDATION.validate("out/${EXEC_PLAN_ID}.csv")
        == "out/${EXEC_PLAN_ID}.csv"
    )


def test_dest_path_rejects_empty():
    with pytest.raises(ConstraintError) as ei:
        _validation.DEST_PATH_VALIDATION.validate("")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_6


def test_dest_path_rejects_glob():
    with pytest.raises(ConstraintError) as ei:
        _validation.DEST_PATH_VALIDATION.validate("out/*.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_9


def test_dest_path_rejects_unknown_token():
    with pytest.raises(ConstraintError) as ei:
        _validation.DEST_PATH_VALIDATION.validate("out/${UNKNOWN_TOKEN}.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_10


def test_dest_path_accepts_backslash_as_separator():
    # A backslash is a valid path separator (normalized per-OS at use time).
    assert _validation.DEST_PATH_VALIDATION.validate("out\\file.csv") == "out\\file.csv"


def test_dest_path_rejects_dot_segments():
    for value in ("..", "out/../file.csv", "out\\..\\file.csv", "./file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.DEST_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_8


def test_dest_path_rejects_leading_slash():
    # A destination path is relative to the connection's base_path; either
    # separator counts as an (illegal) leading anchor.
    for value in ("/out/file.csv", "\\out\\file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.DEST_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_25


def test_dest_path_rejects_trailing_slash():
    for value in ("out/", "out\\"):
        with pytest.raises(ConstraintError) as ei:
            _validation.DEST_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_26


def test_dest_path_rejects_double_slash():
    for value in ("out//file.csv", "out\\\\file.csv"):
        with pytest.raises(ConstraintError) as ei:
            _validation.DEST_PATH_VALIDATION.validate(value)
        assert ei.value.code is FileCommonErrorCode.FILE_COMMON_27


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
            {SrcFileFormat.CSV: "not a dict"},
            side="src",  # type: ignore[dict-item]
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_12
    msg = str(ei.value)
    assert "format_cfg[CSV]" in msg
    assert "got str" in msg


def test_format_cfg_accepts_the_excel_src_key():
    cfg = {SrcFileFormat.EXCEL: {"has_header": False}}
    assert _validation.validate_format_cfg(cfg, side="src") == cfg
    assert _validation.validate_format_cfg({SrcFileFormat.EXCEL: {}}, side="src") == {
        SrcFileFormat.EXCEL: {}
    }


def test_format_cfg_rejects_the_worksheet_as_an_option():
    # The sheet is the path's ``#<n>`` fragment, never a ``format_cfg`` key;
    # a user who writes it here is told so rather than silently ignored.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg({SrcFileFormat.EXCEL: {"sheet": 2}}, side="src")
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_11
    assert "has_header" in str(ei.value)


def test_format_cfg_rejects_a_csv_key_on_excel():
    with pytest.raises(ConnInitException):
        _validation.validate_format_cfg(
            {SrcFileFormat.EXCEL: {"separator": ","}}, side="src"
        )


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


# --- src/dest path NUL byte + bad side ---------------------------------


def test_src_path_rejects_null_byte():
    with pytest.raises(ConstraintError) as ei:
        _validation.SRC_PATH_VALIDATION.validate("dir/has\x00null.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_2


def test_dest_path_rejects_null_byte():
    with pytest.raises(ConstraintError) as ei:
        _validation.DEST_PATH_VALIDATION.validate("dir/has\x00null.csv")
    assert ei.value.code is FileCommonErrorCode.FILE_COMMON_7


def test_format_cfg_rejects_bad_side():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_format_cfg(
            {SrcFileFormat.CSV: {}},
            side="other",
        )
    assert ei.value.api_error is FileCommonErrorCode.FILE_COMMON_13


# --- ebcdic format_cfg ---------------------------------------------------


def _copybook(record: str) -> str:
    """A minimal copybook source. `copybook` carries the COBOL itself, not a
    path to it, so fixtures have to be real (if tiny) layouts."""
    return f"      01  {record}.\n          05  FIELD PIC X(4).\n"


def test_ebcdic_src_cfg_accepts_its_own_keys():
    cfg = {
        SrcFileFormat.EBCDIC: {
            "copybook": [_copybook("CUSTOMER")],
            "codepage": "cp500",
        }
    }
    assert _validation.validate_format_cfg(cfg, side="src") == cfg


@pytest.mark.parametrize(
    "key", ["copybook_text", "keep_filler", "hfp", "trim", "lrecl"]
)
def test_ebcdic_src_cfg_takes_only_copybook_and_codepage(key):
    # The binary has more reader knobs; none is reachable from format_cfg.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_format_cfg(
            {SrcFileFormat.EBCDIC: {"copybook": [_copybook("REC")], key: "x"}},
            side="src",
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_11


def test_ebcdic_src_cfg_rejects_an_unknown_key():
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_format_cfg(
            {SrcFileFormat.EBCDIC: {"lrecl": 80}}, side="src"
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_11


def test_ebcdic_is_source_only_on_the_dest_side():
    # There is no DestFileFormat.EBCDIC, so a dest cfg cannot name it at all.
    assert not hasattr(DestFileFormat, "EBCDIC")


# --- validate_src_format -------------------------------------------------


def _ebcdic(**cfg) -> dict:
    return {SrcFileFormat.EBCDIC: cfg}


def test_validate_src_format_accepts_one_copybook_per_path():
    _validation.validate_src_format(
        SrcFileFormat.EBCDIC, ["cust.dat"], _ebcdic(copybook=[_copybook("CUSTOMER")])
    )
    _validation.validate_src_format(
        SrcFileFormat.EBCDIC,
        ["jan.dat", "feb.dat", "mar.dat"],
        _ebcdic(copybook=[_copybook("JAN"), _copybook("FEB"), _copybook("MAR")]),
    )


def test_paths_sharing_a_layout_repeat_the_same_copybook():
    # The pairing is positional, so a shared layout is spelled out.
    _validation.validate_src_format(
        SrcFileFormat.EBCDIC,
        ["jan.dat", "feb.dat"],
        _ebcdic(copybook=[_copybook("CUSTOMER")] * 2),
    )


@pytest.mark.parametrize(
    "cfg", [None, {}, _ebcdic(), _ebcdic(codepage="cp037"), _ebcdic(copybook=[])]
)
def test_an_ebcdic_source_with_no_copybook_fails_at_declaration_time(cfg):
    # The descriptor must not build, rather than the first run failing.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(SrcFileFormat.EBCDIC, ["cust.dat"], cfg)
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_31


@pytest.mark.parametrize(
    "paths, copybooks",
    [
        (["a.dat", "b.dat"], [_copybook("A")]),
        (["a.dat"], [_copybook("A"), _copybook("B")]),
        ([], [_copybook("A")]),
    ],
)
def test_a_copybook_list_that_does_not_match_the_paths_fails(paths, copybooks):
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC, paths, _ebcdic(copybook=copybooks)
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_32
    message = str(excinfo.value)
    assert f"list of {len(paths)} entries" in message
    assert str(len(copybooks)) in message


def test_a_bare_copybook_string_is_not_a_list_of_one():
    # Accepting a scalar would make the 1-1 rule look optional.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC,
            ["cust.dat"],
            _ebcdic(copybook=_copybook("CUSTOMER")),
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_32
    assert "str" in str(excinfo.value)


@pytest.mark.parametrize("path", ["*.dat", "extracts/*.dat", "cust*.dat", "2026/*"])
def test_an_ebcdic_path_may_not_use_a_wildcard(path):
    # An unknown number of files pairs with no fixed list.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC, [path], _ebcdic(copybook=[_copybook("CUSTOMER")])
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_33
    assert path in str(excinfo.value)


def test_the_wildcard_rule_is_reported_before_the_count_rule():
    # Reporting the count first would send the user to fix the wrong thing.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC, ["*.dat", "b.dat"], _ebcdic(copybook=[_copybook("A")])
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_33


def test_a_wildcard_is_fine_for_every_other_format():
    for fmt in (SrcFileFormat.CSV, SrcFileFormat.EXCEL, SrcFileFormat.PARQUET):
        _validation.validate_src_format(fmt, ["*.dat"], None)


@pytest.mark.parametrize(
    "fmt",
    [
        SrcFileFormat.AUTO,
        SrcFileFormat.CSV,
        SrcFileFormat.PARQUET,
        SrcFileFormat.EXCEL,
    ],
)
def test_validate_src_format_leaves_every_other_format_alone(fmt):
    # Only EBCDIC carries a mandatory format_cfg entry.
    _validation.validate_src_format(fmt, ["a.dat"], None)
    _validation.validate_src_format(fmt, ["a.dat"], {})


@pytest.mark.parametrize("codepage", ["cp037", "cp273", "cp500", "cp1140"])
def test_every_supported_codepage_is_accepted(codepage):
    _validation.validate_src_format(
        SrcFileFormat.EBCDIC,
        ["cust.dat"],
        _ebcdic(copybook=[_copybook("CUSTOMER")], codepage=codepage),
    )


@pytest.mark.parametrize(
    "codepage",
    [
        "cp1047",  # a real EBCDIC page, just not one we ship
        "utf-8",
        "037",
        "ibm037",
        "CP037",  # the binary deserializes by name, so case matters
        "",
    ],
)
def test_an_unsupported_codepage_fails_at_declaration_time(codepage):
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC,
            ["cust.dat"],
            _ebcdic(copybook=[_copybook("CUSTOMER")], codepage=codepage),
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_34
    # The message has to name the alternatives; the user cannot guess them.
    assert "cp037, cp273, cp500, cp1140" in str(excinfo.value)


def test_an_omitted_codepage_is_fine():
    # The reader defaults to cp037, so absent is not the same as invalid.
    _validation.validate_src_format(
        SrcFileFormat.EBCDIC, ["cust.dat"], _ebcdic(copybook=[_copybook("CUSTOMER")])
    )


def test_the_copybook_rules_are_reported_before_the_codepage():
    # A bad codepage alongside a missing copybook: the copybook is the thing
    # without which nothing can be read.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EBCDIC, ["cust.dat"], _ebcdic(codepage="nope")
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_31


# --- validate_src_format: worksheet fragments ----------------------------
#
# The `#<sheet>` rules are a path concern, not an EBCDIC one, so they run
# ahead of the format gate and apply to every source format.


@pytest.mark.parametrize("fmt", [SrcFileFormat.AUTO, SrcFileFormat.EXCEL])
def test_validate_src_format_accepts_a_worksheet_fragment(fmt):
    _validation.validate_src_format(fmt, ["book.xlsx#2"], None)


def test_validate_src_format_accepts_paths_without_a_fragment():
    _validation.validate_src_format(SrcFileFormat.CSV, ["rows.csv"], None)


@pytest.mark.parametrize("fragment", ["", "abc", "2.0", "-1", "٢", "4294967296"])
def test_a_malformed_worksheet_fragment_fails_at_declaration_time(fragment):
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.AUTO, [f"book.xlsx#{fragment}"], None
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_28


def test_worksheet_zero_fails_at_declaration_time():
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(SrcFileFormat.AUTO, ["book.xlsx#0"], None)
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_29


def test_a_fragment_on_a_non_excel_format_fails_at_declaration_time():
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(SrcFileFormat.CSV, ["rows.csv#2"], None)
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_30


def test_every_path_is_checked_not_only_the_first():
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(
            SrcFileFormat.EXCEL, ["ok.xlsx#1", "bad.xlsx#0"], None
        )
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_29


def test_a_fragment_is_checked_even_on_an_ebcdic_source():
    # The fragment rules run before the EBCDIC gate returns, so a format that
    # takes no worksheet still reports the fragment rather than the copybook.
    with pytest.raises(ConnInitException) as excinfo:
        _validation.validate_src_format(SrcFileFormat.EBCDIC, ["cust.dat#2"], None)
    assert excinfo.value.api_error is FileCommonErrorCode.FILE_COMMON_30

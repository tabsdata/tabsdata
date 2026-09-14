#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from typing import Any, get_args

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.common.file.error import FileCommonErrorCode

# --- Path rules -----------------------------------------------------
#
# A src / dest path is relative to the connection's `base_path`: it must not
# start or end with a separator (`/` or `\`), or contain empty (`//`, `\\`)
# or dot (`.`, `..`) segments. Either separator is accepted; the runtime
# normalizes to the OS-native separator when composing the real path.
#
# The rules below run in order and the first failure reports its own code,
# so each way a path can be wrong stays separately diagnosable. Rules after
# the non-empty one may index into the value safely.

_TOKEN_RE = re.compile(r"\$\{[^}]*\}")


def _path_detail(value: str) -> dict[str, Any]:
    return {"path": value}


def _scrubbed_detail(value: str) -> dict[str, Any]:
    # NUL never reaches a message: it is replaced so the rendered path stays
    # printable.
    return {"path": value.replace("\x00", "#")}


def _segments(value: str) -> list[str]:
    return re.split(r"[/\\]", value)


def _no_edge_separator(value: str, edge: int) -> bool:
    return value[edge] not in "/\\"


def _no_dot_segments(value: str) -> bool:
    segments = _segments(value)
    return "." not in segments and ".." not in segments


# --- Source path ----------------------------------------------------

SRC_PATH_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(FileCommonErrorCode.FILE_COMMON_1, min_len=1),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_2,
        lambda value: "\x00" not in value,
        _scrubbed_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_22,
        lambda value: _no_edge_separator(value, 0),
        _path_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_23,
        lambda value: _no_edge_separator(value, -1),
        _path_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_24,
        lambda value: "" not in _segments(value),
        _path_detail,
    ),
    _api.Rule(FileCommonErrorCode.FILE_COMMON_3, _no_dot_segments, _path_detail),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_4,
        lambda value: _TOKEN_RE.search(value) is None,
        _path_detail,
    ),
    # A wildcard is allowed in the final segment only -- it selects files,
    # never directories.
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_5,
        lambda value: "*" not in "".join(_segments(value)[:-1]),
        _path_detail,
    ),
)


# --- Destination path -----------------------------------------------


def _dest_tokens_allowed(value: str) -> bool:
    # Local import: `typing_` imports this module, so the token set must be
    # looked up at call time, not at module import.
    from tabsdatak.conn.common.file.typing_ import DEST_PATH_TOKENS

    return all(m.group(0) in DEST_PATH_TOKENS for m in _TOKEN_RE.finditer(value))


def _dest_token_detail(value: str) -> dict[str, Any]:
    from tabsdatak.conn.common.file.typing_ import DEST_PATH_TOKENS

    bad = next(
        (
            m.group(0)
            for m in _TOKEN_RE.finditer(value)
            if m.group(0) not in DEST_PATH_TOKENS
        ),
        None,
    )
    return {"path": value, "token": bad, "allowed": sorted(DEST_PATH_TOKENS)}


DEST_PATH_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(FileCommonErrorCode.FILE_COMMON_6, min_len=1),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_7,
        lambda value: "\x00" not in value,
        _scrubbed_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_25,
        lambda value: _no_edge_separator(value, 0),
        _path_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_26,
        lambda value: _no_edge_separator(value, -1),
        _path_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_27,
        lambda value: "" not in _segments(value),
        _path_detail,
    ),
    _api.Rule(FileCommonErrorCode.FILE_COMMON_8, _no_dot_segments, _path_detail),
    # A destination path names one file, so no wildcard at all.
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_9,
        lambda value: "*" not in value,
        _path_detail,
    ),
    _api.Rule(
        FileCommonErrorCode.FILE_COMMON_10, _dest_tokens_allowed, _dest_token_detail
    ),
)


# --- format_cfg key whitelisting ------------------------------------


def _allowed_keys(fmt_name: str, *, side: str) -> frozenset[str]:
    # Local import for the same circular-import reason as above.
    from tabsdatak.conn.common.file.typing_ import (
        CsvDestKey,
        CsvSrcKey,
        EbcdicSrcKey,
        ExcelSrcKey,
    )

    if fmt_name == "CSV":
        if side == "src":
            return frozenset(get_args(CsvSrcKey))
        if side == "dest":
            return frozenset(get_args(CsvDestKey))
    if fmt_name == "EBCDIC" and side == "src":
        return frozenset(get_args(EbcdicSrcKey))
    if fmt_name == "EXCEL" and side == "src":
        # Deliberately not ``sheet``: that arrives as a path fragment, not
        # as an option, and a user naming it here should be told so.
        return frozenset(get_args(ExcelSrcKey))
    # JSON / AVRO / PARQUET / LOG / AUTO: no keys accepted today.
    return frozenset()


def validate_format_cfg(
    value: dict[Any, dict[str, Any]] | None,
    *,
    side: str,
) -> dict[Any, dict[str, Any]] | None:
    if value is None:
        return value
    if side not in ("src", "dest"):
        raise FileCommonErrorCode.FILE_COMMON_13.exception(side=side)
    for fmt, cfg in value.items():
        # ``fmt`` is a SrcFileFormat or DestFileFormat instance; pull
        # its name for the whitelist lookup.
        fmt_name = getattr(fmt, "name", str(fmt))
        if not isinstance(cfg, dict):
            raise FileCommonErrorCode.FILE_COMMON_12.exception(
                format=fmt_name,
                got_type=type(cfg).__name__,
            )
        allowed = _allowed_keys(fmt_name, side=side)
        unknown = set(cfg) - allowed
        if unknown:
            raise FileCommonErrorCode.FILE_COMMON_11.exception(
                format=fmt_name,
                side=side,
                unknown=sorted(unknown),
                allowed=sorted(allowed) or "(none)",
            )
    return value


# The wire carries the worksheet number as a u32, so a fragment above this
# is malformed rather than merely out of range -- which sheet a workbook
# actually has is the binary's to report, against the workbook.
_MAX_SHEET = 0xFFFF_FFFF


def split_sheet_fragment(path: str, file_format: Any) -> tuple[str, int | None]:
    """Split a source path's ``#<sheet>`` worksheet fragment off.

    Returns the fragment-free path and the 1-based worksheet number the
    fragment named, or ``None`` when there was none. Splitting on the
    *first* ``#`` matches what ``Url::parse`` already does to the uri
    inside the binary, so a literal ``#`` in an object key is no more
    expressible than it was before this existed.

    Everything downstream -- ``build_location``, AUTO inference, the
    transporter's own output naming -- reads the path, so this runs before
    any of them and they never see a fragment.

    A fragment is only accepted under ``EXCEL`` or ``AUTO``; under AUTO a
    path that turns out not to be a workbook is caught once the format
    resolves, which is the first point that can know.

    :func:`validate_src_format` runs this over every path, so a bad
    fragment fails the descriptor; the reader calls it again only to read
    the number back off an already-validated path.
    """
    # Local import for the same circular-import reason as above.
    from tabsdatak.common._fileformat.taxonomy import SrcFileFormat

    head, marker, fragment = path.partition("#")
    if not marker:
        return path, None
    if file_format not in (SrcFileFormat.AUTO, SrcFileFormat.EXCEL):
        raise FileCommonErrorCode.FILE_COMMON_30.exception(
            path=path, format=file_format.name
        )
    # ``str.isdigit`` alone admits superscripts and other unicode digits
    # that ``int`` then refuses, so ASCII is required too.
    if not (fragment.isascii() and fragment.isdigit()) or int(fragment) > _MAX_SHEET:
        raise FileCommonErrorCode.FILE_COMMON_28.exception(path=path, fragment=fragment)
    sheet = int(fragment)
    if sheet == 0:
        raise FileCommonErrorCode.FILE_COMMON_29.exception(path=path)
    return head, sheet


def validate_src_format(fmt: Any, paths: Any, value: dict | None) -> None:
    """Cross-check a source's `format` against its `paths` and `format_cfg`.

    For Excel format, the paths are checked for a `#<sheet>` fragment and the fragment
    is validated. For EBCDIC format, the paths are checked for a `copybook` entry in the
    `format_cfg`, the number of copybooks must match the number of paths, and the
    `codepage` is validated for valid values.
    """
    # Local import for the same circular-import reason as above.
    from tabsdatak.common._fileformat.taxonomy import (
        EBCDIC_CODEPAGES,
        SrcFileFormat,
    )

    # Excel paths are checked for a `#<sheet>` fragment and the fragment is validated.
    for path in paths or []:
        split_sheet_fragment(str(path), fmt)

    if fmt is not SrcFileFormat.EBCDIC:
        return
    cfg = (value or {}).get(SrcFileFormat.EBCDIC) or {}
    copybooks = cfg.get("copybook")
    if not copybooks:
        raise FileCommonErrorCode.FILE_COMMON_31.exception()

    entries = list(paths or [])
    # A wildcard is an unknown number of files, so no fixed list pairs with
    # it. Before the count check, which would report an unactionable length.
    for path in entries:
        if "*" in str(path):
            raise FileCommonErrorCode.FILE_COMMON_33.exception(path=path)

    if not isinstance(copybooks, (list, tuple)) or len(copybooks) != len(entries):
        given = (
            f"{len(copybooks)}"
            if isinstance(copybooks, (list, tuple))
            else type(copybooks).__name__
        )
        raise FileCommonErrorCode.FILE_COMMON_32.exception(
            paths=len(entries), given=given
        )

    codepage = cfg.get("codepage")
    # Spelled exactly as the binary's enum spells it -- it deserializes by
    # name, so `CP037` is as unusable as `cp1047`.
    if codepage is not None and codepage not in EBCDIC_CODEPAGES:
        raise FileCommonErrorCode.FILE_COMMON_34.exception(
            codepage=codepage, supported=", ".join(EBCDIC_CODEPAGES)
        )

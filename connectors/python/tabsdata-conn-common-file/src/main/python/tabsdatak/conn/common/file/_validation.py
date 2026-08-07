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
    from tabsdatak.conn.common.file.typing_ import CsvDestKey, CsvSrcKey

    if fmt_name == "CSV":
        if side == "src":
            return frozenset(get_args(CsvSrcKey))
        if side == "dest":
            return frozenset(get_args(CsvDestKey))
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

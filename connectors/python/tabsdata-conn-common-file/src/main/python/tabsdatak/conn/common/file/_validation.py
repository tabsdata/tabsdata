from __future__ import annotations

import re
from typing import Any, get_args

from pydantic import AfterValidator

from tabsdatak.conn.common.file.error import FileCommonErrorCode

# --- Source path ----------------------------------------------------

_TOKEN_RE = re.compile(r"\$\{[^}]*\}")


def validate_src_path(value: str) -> str:
    if not value:
        raise FileCommonErrorCode.FILE_COMMON_1.exception()
    if "\x00" in value:
        value = value.replace("\x00", "#")
        raise FileCommonErrorCode.FILE_COMMON_2.exception(path=value)
    if "\\" in value:
        raise FileCommonErrorCode.FILE_COMMON_3.exception(path=value)
    if _TOKEN_RE.search(value):
        raise FileCommonErrorCode.FILE_COMMON_4.exception(path=value)
    if "*" in value:
        head, _, _basename = value.rpartition("/")
        if "*" in head:
            raise FileCommonErrorCode.FILE_COMMON_5.exception(path=value)
    return value


SRC_PATH_VALIDATOR = AfterValidator(validate_src_path)


# --- Destination path -----------------------------------------------


def validate_dest_path(value: str) -> str:
    # Local import: types.py imports the validators defined above,
    # so the constants must be looked up after typing_ has finished
    # loading (i.e. at call time, not at module import).
    from tabsdatak.conn.common.file.typing_ import DEST_PATH_TOKENS

    if not value:
        raise FileCommonErrorCode.FILE_COMMON_6.exception()
    if "\x00" in value:
        value = value.replace("\x00", "#")
        raise FileCommonErrorCode.FILE_COMMON_7.exception(path=value)
    if "\\" in value:
        raise FileCommonErrorCode.FILE_COMMON_8.exception(path=value)
    if "*" in value:
        raise FileCommonErrorCode.FILE_COMMON_9.exception(path=value)
    for m in _TOKEN_RE.finditer(value):
        token = m.group(0)
        if token not in DEST_PATH_TOKENS:
            raise FileCommonErrorCode.FILE_COMMON_10.exception(
                path=value,
                token=token,
                allowed=sorted(DEST_PATH_TOKENS),
            )
    return value


DEST_PATH_VALIDATOR = AfterValidator(validate_dest_path)


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

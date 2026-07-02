from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class S3ValidateException(ConnException):
    """Cross-field validation failure for the S3 connector."""


class S3RuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the S3 connector."""


class S3ErrorCode(ErrorCode):
    S3_1 = ErrorDef(
        ConnInitException,
        "S3 connection validation failed",
    )  # S3SrcConn / S3DestConn
    S3_2 = ErrorDef(
        ConnInitException,
        "S3Src validation failed",
    )  # S3Src
    S3_3 = ErrorDef(
        S3ValidateException,
        "S3Src.format_cfg ({side}-side) cross-field validation failed",
    )  # S3Src.validate()
    S3_4 = ErrorDef(
        ConnInitException,
        "S3Dest validation failed",
    )  # S3Dest
    S3_5 = ErrorDef(
        S3ValidateException,
        "S3Dest.format_cfg ({side}-side) cross-field validation failed",
    )  # S3Dest.validate()
    S3_6 = ErrorDef(
        S3RuntimeException,
        "td_transporter binary not found in the runtime environment.",
    )  # _S3SrcPlugin / _S3DestPlugin: binary lookup
    S3_7 = ErrorDef(
        S3RuntimeException,
        "S3 read failed (path={path})",
    )  # _S3SrcPlugin: import_one failure
    S3_8 = ErrorDef(
        S3RuntimeException,
        "S3 write failed (target={target})",
    )  # _S3DestPlugin: copy_one failure
    S3_9 = ErrorDef(
        S3RuntimeException,
        "S3 transporter report invalid (path={path})",
    )  # _S3SrcPlugin / _S3DestPlugin: bad report YAML
    S3_10 = ErrorDef(
        S3RuntimeException,
        "S3 credential resolution failed: {reason}",
    )  # _S3SrcPlugin / _S3DestPlugin: Secret-backed creds w/o resolver

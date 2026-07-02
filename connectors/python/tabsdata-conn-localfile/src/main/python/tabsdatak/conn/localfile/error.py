from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class LocalFileValidateException(ConnException):
    """Cross-field validation failure for the LocalFile connector."""


class LocalFileRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the LocalFile connector."""


class LocalFileErrorCode(ErrorCode):
    LOCALFILE_1 = ErrorDef(
        ConnInitException,
        "LocalFile connection validation failed",
    )  # LocalFileSrcConn / LocalFileDestConn
    LOCALFILE_2 = ErrorDef(
        ConnInitException,
        "LocalFileSrc validation failed",
    )  # LocalFileSrc
    LOCALFILE_3 = ErrorDef(
        LocalFileValidateException,
        "LocalFileSrc.format_cfg cross-field validation failed",
    )  # LocalFileSrc.validate()
    LOCALFILE_4 = ErrorDef(
        ConnInitException,
        "LocalFileDest validation failed",
    )  # LocalFileDest
    LOCALFILE_5 = ErrorDef(
        LocalFileValidateException,
        "LocalFileDest.format_cfg cross-field validation failed",
    )  # LocalFileDest.validate()
    LOCALFILE_6 = ErrorDef(
        LocalFileRuntimeException,
        "td_transporter binary not found in the runtime environment.",
    )  # _LocalFileSrcPlugin / _LocalFileDestPlugin: binary lookup
    LOCALFILE_7 = ErrorDef(
        LocalFileRuntimeException,
        "LocalFile read failed (path={path})",
    )  # _LocalFileSrcPlugin: import_one failure
    LOCALFILE_8 = ErrorDef(
        LocalFileRuntimeException,
        "LocalFile write failed (target={target})",
    )  # _LocalFileDestPlugin: copy_one failure
    LOCALFILE_9 = ErrorDef(
        LocalFileRuntimeException,
        "LocalFile transporter report invalid (path={path})",
    )  # _LocalFileSrcPlugin / _LocalFileDestPlugin: bad report YAML

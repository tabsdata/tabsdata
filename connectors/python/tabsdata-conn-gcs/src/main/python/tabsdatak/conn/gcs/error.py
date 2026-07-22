#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class GcsValidateException(ConnException):
    """Cross-field validation failure for the GCS connector.

    :categories: error
    """


class GcsRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the GCS connector.

    :categories: error
    """


class GcsErrorCode(ErrorCode):
    """Error codes for the GCS connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `GcsErrorCode.GCS_N.exception`.
    """

    GCS_1 = ErrorDef(
        ConnInitException,
        "GCS connection validation failed",
    )  # GCSSrcConn / GCSDestConn
    GCS_2 = ErrorDef(
        ConnInitException,
        "GCSSrc validation failed",
    )  # GCSSrc
    GCS_3 = ErrorDef(
        GcsValidateException,
        "GCSSrc.format_cfg ({side}-side) cross-field validation failed",
    )  # GCSSrc.validate()
    GCS_4 = ErrorDef(
        ConnInitException,
        "GCSDest validation failed",
    )  # GCSDest
    GCS_5 = ErrorDef(
        GcsValidateException,
        "GCSDest.format_cfg ({side}-side) cross-field validation failed",
    )  # GCSDest.validate()
    GCS_6 = ErrorDef(
        GcsRuntimeException,
        "td_transporter binary not found in the runtime environment.",
    )  # _GCSSrcPlugin / _GCSDestPlugin: binary lookup
    GCS_7 = ErrorDef(
        GcsRuntimeException,
        "GCS read failed (path={path})",
    )  # _GCSSrcPlugin: import_one failure
    GCS_8 = ErrorDef(
        GcsRuntimeException,
        "GCS write failed (target={target})",
    )  # _GCSDestPlugin: copy_one failure
    GCS_9 = ErrorDef(
        GcsRuntimeException,
        "GCS transporter report invalid (path={path})",
    )  # _GCSSrcPlugin / _GCSDestPlugin: bad report YAML
    GCS_10 = ErrorDef(
        GcsRuntimeException,
        "GCS credential resolution failed: {reason}",
    )  # _GCSSrcPlugin / _GCSDestPlugin: unresolved credential ref

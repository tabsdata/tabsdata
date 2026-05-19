from __future__ import annotations

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class DruidRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Druid connector."""


class DruidErrorCode(ErrorCode):
    DRUID_1 = ErrorDef(
        ConnInitException,
        "DruidConn validation failed",
    )  # DruidConn
    DRUID_2 = ErrorDef(
        ConnInitException,
        "DruidDest validation failed",
    )  # DruidDest
    DRUID_3 = ErrorDef(
        ConnInitException,
        "Druid datasource must be 1-128 chars (got {length}): {value!r}",
    )  # _validate_datasource: bad length
    DRUID_4 = ErrorDef(
        ConnInitException,
        "Druid datasource may not start with '.' or '_': {value!r}",
    )  # _validate_datasource: leading '.' / '_'
    DRUID_5 = ErrorDef(
        ConnInitException,
        "Druid datasource may not contain '/', '\\\\' or null: {value!r}",
    )  # _validate_datasource: forbidden char
    DRUID_6 = ErrorDef(
        DruidRuntimeException,
        "DruidDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _DruidDestPlugin: defensive recheck
    DRUID_7 = ErrorDef(
        DruidRuntimeException,
        "Druid staging upload failed (datasource={datasource})",
    )  # _DruidDestPlugin.write_out: staging upload
    DRUID_8 = ErrorDef(
        DruidRuntimeException,
        "Druid ingestion task submission failed "
        "(datasource={datasource}, url={url})",
    )  # _DruidDestPlugin.write_out: HTTP submit
    DRUID_9 = ErrorDef(
        DruidRuntimeException,
        "Druid ingestion task failed "
        "(datasource={datasource}, task_id={task_id}): {reason}",
    )  # _DruidDestPlugin.write_out: poll returned FAILED
    DRUID_10 = ErrorDef(
        DruidRuntimeException,
        "Druid Overlord response malformed (url={url}): {reason}",
    )  # _DruidDestPlugin.write_out: bad payload from Overlord
    DRUID_11 = ErrorDef(
        DruidRuntimeException,
        "Druid staging location {kind!r} not supported by this connector",
    )  # _DruidDestPlugin.write_out: staging variant the plugin doesn't know

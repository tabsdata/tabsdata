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


class MSSQLValidateException(ConnException):
    """Cross-field validation failure for the MSSQL connector.

    :categories: error
    """


class MSSQLRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the MSSQL connector.

    :categories: error
    """


class MssqlErrorCode(ErrorCode):
    """Error codes for the MSSQL connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `MssqlErrorCode.MSSQL_N.exception`.
    """

    MSSQL_1 = ErrorDef(
        ConnInitException,
        "MSSQL connection validation failed",
    )  # MSSQLSrcConn / MSSQLDestConn
    MSSQL_2 = ErrorDef(
        ConnInitException,
        "MSSQLSrc validation failed",
    )  # MSSQLSrc
    MSSQL_3 = ErrorDef(
        MSSQLValidateException,
        "MSSQLSrc.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # MSSQLSrc.validate()
    MSSQL_4 = ErrorDef(
        ConnInitException,
        "MSSQLDest validation failed",
    )  # MSSQLDest
    MSSQL_5 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL source connection failed (uri={uri})",
    )  # _MSSQLSrcPlugin.read_in: engine / connect failure
    MSSQL_6 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL query failed (query_index={query_index})",
    )  # _MSSQLSrcPlugin: read failure inside read_in
    MSSQL_7 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQLDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _MSSQLDestPlugin: defensive recheck
    MSSQL_8 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL destination connection failed (uri={uri})",
    )  # _MSSQLDestPlugin.write_out: engine / connect failure
    MSSQL_9 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL write failed (table={table})",
    )  # _MSSQLDestPlugin: write failure inside write_out

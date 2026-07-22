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


class OracleValidateException(ConnException):
    """Cross-field validation failure for the Oracle connector.

    :categories: error
    """


class OracleRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Oracle connector.

    :categories: error
    """


class OracleErrorCode(ErrorCode):
    """Error codes for the Oracle connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `OracleErrorCode.ORACLE_N.exception`.
    """

    ORACLE_1 = ErrorDef(
        ConnInitException,
        "Oracle connection validation failed",
    )  # OracleSrcConn / OracleDestConn
    ORACLE_2 = ErrorDef(
        ConnInitException,
        "OracleSrc validation failed",
    )  # OracleSrc
    ORACLE_3 = ErrorDef(
        OracleValidateException,
        "OracleSrc.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # OracleSrc.validate()
    ORACLE_4 = ErrorDef(
        ConnInitException,
        "OracleDest validation failed",
    )  # OracleDest
    ORACLE_5 = ErrorDef(
        OracleRuntimeException,
        "Oracle source connection failed (uri={uri})",
    )  # _OracleSrcPlugin.read_in: engine / connect failure
    ORACLE_6 = ErrorDef(
        OracleRuntimeException,
        "Oracle query failed (query_index={query_index})",
    )  # _OracleSrcPlugin: read failure inside read_in
    ORACLE_7 = ErrorDef(
        OracleRuntimeException,
        "OracleDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _OracleDestPlugin: defensive recheck
    ORACLE_8 = ErrorDef(
        OracleRuntimeException,
        "Oracle destination connection failed (uri={uri})",
    )  # _OracleDestPlugin.write_out: engine / connect failure
    ORACLE_9 = ErrorDef(
        OracleRuntimeException,
        "Oracle write failed (table={table})",
    )  # _OracleDestPlugin: write failure inside write_out

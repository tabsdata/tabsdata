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


class SnowflakeValidateException(ConnException):
    """Cross-field validation failure for the Snowflake connector.

    :categories: error
    """


class SnowflakeRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Snowflake connector.

    :categories: error
    """


class SnowflakeErrorCode(ErrorCode):
    """Error codes for the Snowflake connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `SnowflakeErrorCode.SNOWFLAKE_N.exception`.
    """

    SNOWFLAKE_1 = ErrorDef(
        ConnInitException,
        "SnowflakeDestConn validation failed",
    )  # SnowflakeDestConn
    SNOWFLAKE_2 = ErrorDef(
        SnowflakeValidateException,
        "SnowflakeDestConn.schema={schema!r} requires database to be set",
    )  # SnowflakeDestConn.validate()
    SNOWFLAKE_3 = ErrorDef(
        ConnInitException,
        "SnowflakeDest validation failed",
    )  # SnowflakeDest
    SNOWFLAKE_4 = ErrorDef(
        SnowflakeRuntimeException,
        "SnowflakeDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _SnowflakeDestPlugin: defensive recheck
    SNOWFLAKE_5 = ErrorDef(
        SnowflakeRuntimeException,
        "Snowflake connection failed (account={account})",
    )  # _SnowflakeDestPlugin: engine / connect failure
    SNOWFLAKE_6 = ErrorDef(
        SnowflakeRuntimeException,
        "Snowflake write failed (table={table})",
    )  # _SnowflakeDestPlugin: write failure inside write_out
    SNOWFLAKE_7 = ErrorDef(
        SnowflakeRuntimeException,
        "Snowflake session could not adopt database={database!r} schema={schema!r}",
    )  # write_all_tables: USE DATABASE / USE SCHEMA failed
    SNOWFLAKE_8 = ErrorDef(
        SnowflakeRuntimeException,
        "The Snowflake bulk load cannot carry binary values, so table"
        " {table!r} cannot take the column(s) {columns}. Encode them before"
        " writing -- for example base64 into a string",
    )  # write_all_tables: parquet binary column, rejected before loading

    # --- Field-value rules (tabsdatak.conn.snowflake) ---------------
    SNOWFLAKE_9 = ErrorDef(
        ConnInitException,
        "not a valid Snowflake account identifier (letters, digits, dots, "
        "underscores and hyphens, starting and ending alphanumeric): {value!r}",
    )  # AccountSpec
    SNOWFLAKE_10 = ErrorDef(
        ConnInitException,
        "not a valid Snowflake identifier (unquoted letters, digits, underscore "
        "and $, or a double-quoted name): {value!r}",
    )  # IdentifierSpec

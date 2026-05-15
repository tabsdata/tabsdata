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
    """Cross-field validation failure for the Snowflake connector."""


class SnowflakeRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Snowflake connector."""


class SnowflakeErrorCode(ErrorCode):
    SNOWFLAKE_1 = ErrorDef(
        ConnInitException,
        "SnowflakeConn validation failed",
    )  # SnowflakeConn
    SNOWFLAKE_2 = ErrorDef(
        SnowflakeValidateException,
        "SnowflakeConn.schema={schema!r} requires database to be set",
    )  # SnowflakeConn.validate()
    SNOWFLAKE_3 = ErrorDef(
        ConnInitException,
        "SnowflakeDest validation failed",
    )  # SnowflakeDest
    SNOWFLAKE_4 = ErrorDef(
        ConnInitException,
        "Snowflake identifier must be non-empty",
    )  # _validate_identifier: empty
    SNOWFLAKE_5 = ErrorDef(
        ConnInitException,
        "Snowflake account must be non-empty",
    )  # _validate_account: empty
    SNOWFLAKE_6 = ErrorDef(
        SnowflakeRuntimeException,
        "Snowflake connection failed (account={account})",
    )  # _SnowflakeDestPlugin: engine / connect failure
    SNOWFLAKE_7 = ErrorDef(
        SnowflakeRuntimeException,
        "Snowflake write failed (table={table})",
    )  # _SnowflakeDestPlugin: write failure inside write_out
    SNOWFLAKE_8 = ErrorDef(
        SnowflakeRuntimeException,
        "SnowflakeDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _SnowflakeDestPlugin: defensive recheck
    SNOWFLAKE_9 = ErrorDef(
        ConnInitException,
        "Snowflake identifier exceeds {max} chars: {value!r}",
    )  # _validate_identifier: too long
    SNOWFLAKE_10 = ErrorDef(
        ConnInitException,
        'double-quoted Snowflake identifier has unescaped " : {value!r}',
    )  # _validate_identifier: double-quoted bad
    SNOWFLAKE_11 = ErrorDef(
        ConnInitException,
        "double-quoted Snowflake identifier must be non-empty: {value!r}",
    )  # _validate_identifier: double-quoted empty
    SNOWFLAKE_12 = ErrorDef(
        ConnInitException,
        "unquoted Snowflake identifier must match [A-Za-z_][A-Za-z0-9_$]*: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    SNOWFLAKE_13 = ErrorDef(
        ConnInitException,
        "Snowflake account must be alphanumeric with optional"
        " '.', '-', '_' separators: {value!r}",
    )  # _validate_account: bad shape
    SNOWFLAKE_14 = ErrorDef(
        ConnInitException,
        "Snowflake table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    SNOWFLAKE_15 = ErrorDef(
        ConnInitException,
        "Snowflake table name must be one of 'table', 'schema.table',"
        " 'database.schema.table': {value!r}",
    )  # _validate_table_fqn: bad arity

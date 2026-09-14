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
    SNOWFLAKE_9 = ErrorDef(
        ConnInitException,
        "not a valid Snowflake account identifier (alphanumerics with dots,"
        " hyphens and underscores, starting and ending alphanumeric):"
        " {value!r}",
    )  # AccountSpec
    SNOWFLAKE_10 = ErrorDef(
        ConnInitException,
        "not a valid Snowflake identifier (unquoted: a letter or underscore"
        " followed by letters, digits, underscores or dollar signs; or"
        " double-quoted for a case-sensitive / special-char name): {value!r}",
    )  # IdentifierSpec / NameSpec
    SNOWFLAKE_11 = ErrorDef(
        ConnInitException,
        "MergeCfg validation failed",
    )  # MergeCfg
    SNOWFLAKE_12 = ErrorDef(
        SnowflakeValidateException,
        'SnowflakeDest.if_table_exists="merge" requires merge_cfg to be set',
    )  # SnowflakeDest.validate()
    SNOWFLAKE_13 = ErrorDef(
        SnowflakeValidateException,
        "SnowflakeDest.merge_cfg is set but if_table_exists={if_table_exists!r};"
        ' merge_cfg only applies to if_table_exists="merge"',
    )  # SnowflakeDest.validate()
    SNOWFLAKE_14 = ErrorDef(
        SnowflakeValidateException,
        "SnowflakeDest.merge_cfg has {cfgs} entries but tables has {tables}; a"
        " list of merge configs must line up one-to-one with tables (pass a"
        " single MergeCfg to apply one config to every table)",
    )  # SnowflakeDest.validate()
    SNOWFLAKE_16 = ErrorDef(
        SnowflakeRuntimeException,
        "Table {table} shares no column with the incoming data, so a merge onto"
        " it would write nothing. Check the target name and that the frame's"
        " columns match the table's",
    )  # _merge_into: nothing to write
    SNOWFLAKE_18 = ErrorDef(
        SnowflakeRuntimeException,
        "MergeCfg.op_column={column!r} is not a column of the data written to"
        " table {table}, so the merge cannot read each row's i/u/d marker. Every"
        " incoming row has to carry it",
    )  # _coalesced_parquet / _written_columns: op column absent from the data
    SNOWFLAKE_19 = ErrorDef(
        SnowflakeRuntimeException,
        "MergeCfg.on names {columns}, which the data written to table {table}"
        " does not carry, so incoming rows cannot be matched against it. Every"
        " key column has to be a column of the frame",
    )  # _coalesced_parquet: key column absent from the incoming data
    SNOWFLAKE_20 = ErrorDef(
        SnowflakeRuntimeException,
        "MergeCfg column {column!r} matches {matches} of the data written to"
        " table {table}. The Snowflake load matches columns case-insensitively,"
        " so columns whose names differ only in case cannot be told apart",
    )  # _coalesced_parquet: ambiguous case-insensitive match

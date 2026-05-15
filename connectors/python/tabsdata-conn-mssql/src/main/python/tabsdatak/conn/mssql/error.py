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
    """Cross-field validation failure for the MSSQL connector."""


class MSSQLRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the MSSQL connector."""


class MssqlErrorCode(ErrorCode):
    MSSQL_1 = ErrorDef(
        ConnInitException,
        "MSSQLConn validation failed",
    )  # MSSQLConn
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
        ConnInitException,
        "MSSQL identifier must be non-empty",
    )  # _validate_identifier: empty
    MSSQL_6 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL source connection failed (uri={uri})",
    )  # _MSSQLSrcPlugin.read_in: engine / connect failure
    MSSQL_7 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL query failed (query_index={query_index})",
    )  # _MSSQLSrcPlugin: read failure inside read_in
    MSSQL_8 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL write failed (table={table})",
    )  # _MSSQLDestPlugin: write failure inside write_out
    MSSQL_9 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQLDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _MSSQLDestPlugin: defensive recheck
    MSSQL_10 = ErrorDef(
        MSSQLRuntimeException,
        "MSSQL destination connection failed (uri={uri})",
    )  # _MSSQLDestPlugin.write_out: engine / connect failure
    MSSQL_11 = ErrorDef(
        ConnInitException,
        "MSSQL identifier exceeds {max} chars: {value!r}",
    )  # _validate_identifier: too long
    MSSQL_12 = ErrorDef(
        ConnInitException,
        "MSSQL bracketed identifier has unescaped ']': {value!r}",
    )  # _validate_identifier: bracket bad
    MSSQL_13 = ErrorDef(
        ConnInitException,
        "MSSQL bracketed identifier must be non-empty: {value!r}",
    )  # _validate_identifier: bracket empty
    MSSQL_14 = ErrorDef(
        ConnInitException,
        'MSSQL double-quoted identifier has unescaped " : {value!r}',
    )  # _validate_identifier: double-quoted bad
    MSSQL_15 = ErrorDef(
        ConnInitException,
        "MSSQL double-quoted identifier must be non-empty: {value!r}",
    )  # _validate_identifier: double-quoted empty
    MSSQL_16 = ErrorDef(
        ConnInitException,
        "regular MSSQL identifier must start with a letter / _ / @ / # and"
        " contain only letters, digits, _, @, #, $: {value!r}",
    )  # _validate_identifier: regular bad shape
    MSSQL_17 = ErrorDef(
        ConnInitException,
        "MSSQL URI must start with mssql:// (optionally driver-qualified,"
        " e.g. mssql+pyodbc://): {value!r}",
    )  # _validate_uri
    MSSQL_18 = ErrorDef(
        ConnInitException,
        "MSSQL table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    MSSQL_19 = ErrorDef(
        ConnInitException,
        "MSSQL table name must be 'table', 'schema.table', or"
        " 'database.schema.table': {value!r}",
    )  # _validate_table_fqn: bad arity

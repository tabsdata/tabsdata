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
    """Cross-field validation failure for the Oracle connector."""


class OracleRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Oracle connector."""


class OracleErrorCode(ErrorCode):
    ORACLE_1 = ErrorDef(
        ConnInitException,
        "OracleConn validation failed",
    )  # OracleConn
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
        ConnInitException,
        "Oracle identifier must be non-empty",
    )  # _validate_identifier: empty
    ORACLE_6 = ErrorDef(
        ConnInitException,
        "Oracle URI must start with oracle:// (optionally driver-qualified,"
        " e.g. oracle+oracledb://): {value!r}",
    )  # _validate_uri
    ORACLE_7 = ErrorDef(
        OracleRuntimeException,
        "Oracle source connection failed (uri={uri})",
    )  # _OracleSrcPlugin.read_in: engine / connect failure
    ORACLE_8 = ErrorDef(
        OracleRuntimeException,
        "Oracle query failed (query_index={query_index})",
    )  # _OracleSrcPlugin: read failure inside read_in
    ORACLE_9 = ErrorDef(
        OracleRuntimeException,
        "Oracle write failed (table={table})",
    )  # _OracleDestPlugin: write failure inside write_out
    ORACLE_10 = ErrorDef(
        OracleRuntimeException,
        "OracleDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _OracleDestPlugin: defensive recheck
    ORACLE_11 = ErrorDef(
        OracleRuntimeException,
        "Oracle destination connection failed (uri={uri})",
    )  # _OracleDestPlugin.write_out: engine / connect failure
    ORACLE_12 = ErrorDef(
        ConnInitException,
        "Oracle identifier exceeds {max} bytes: {value!r}",
    )  # _validate_identifier: too long
    ORACLE_13 = ErrorDef(
        ConnInitException,
        'quoted Oracle identifier has unescaped " : {value!r}',
    )  # _validate_identifier: quoted bad
    ORACLE_14 = ErrorDef(
        ConnInitException,
        "quoted Oracle identifier must be non-empty: {value!r}",
    )  # _validate_identifier: quoted empty
    ORACLE_15 = ErrorDef(
        ConnInitException,
        "ordinary Oracle identifier must match [A-Za-z][A-Za-z0-9_$#]*: {value!r}",
    )  # _validate_identifier: ordinary bad shape
    ORACLE_16 = ErrorDef(
        ConnInitException,
        "Oracle table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    ORACLE_17 = ErrorDef(
        ConnInitException,
        "Oracle table name must be one of 'table', 'schema.table': {value!r}",
    )  # _validate_table_fqn: bad arity

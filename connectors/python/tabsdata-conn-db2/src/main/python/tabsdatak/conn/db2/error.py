from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class Db2ValidateException(ConnException):
    """Cross-field validation failure for the Db2 connector."""


class Db2RuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Db2 connector."""


class Db2ErrorCode(ErrorCode):
    DB2_1 = ErrorDef(
        ConnInitException,
        "Db2Conn validation failed",
    )  # Db2Conn
    DB2_2 = ErrorDef(
        ConnInitException,
        "Db2Src validation failed",
    )  # Db2Src
    DB2_3 = ErrorDef(
        Db2ValidateException,
        "Db2Src.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # Db2Src.validate()
    DB2_4 = ErrorDef(
        ConnInitException,
        "Db2Dest validation failed",
    )  # Db2Dest
    DB2_5 = ErrorDef(
        ConnInitException,
        "Db2 identifier must be non-empty",
    )  # _validate_identifier: empty
    DB2_6 = ErrorDef(
        ConnInitException,
        "Db2 URI must start with db2:// or ibm_db_sa:// (optionally"
        " driver-qualified, e.g. db2+ibm_db://): {value!r}",
    )  # _validate_uri
    DB2_7 = ErrorDef(
        Db2RuntimeException,
        "Db2 source connection failed (uri={uri})",
    )  # _Db2SrcPlugin.read_in: engine / connect failure
    DB2_8 = ErrorDef(
        Db2RuntimeException,
        "Db2 query failed (query_index={query_index})",
    )  # _Db2SrcPlugin: read failure inside read_in
    DB2_9 = ErrorDef(
        Db2RuntimeException,
        "Db2 write failed (table={table})",
    )  # _Db2DestPlugin: write failure inside write_out
    DB2_10 = ErrorDef(
        Db2RuntimeException,
        "Db2Dest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _Db2DestPlugin: defensive recheck
    DB2_11 = ErrorDef(
        Db2RuntimeException,
        "Db2 destination connection failed (uri={uri})",
    )  # _Db2DestPlugin.write_out: engine / connect failure
    DB2_12 = ErrorDef(
        ConnInitException,
        "Db2 identifier exceeds {max} bytes: {value!r}",
    )  # _validate_identifier: too long
    DB2_13 = ErrorDef(
        ConnInitException,
        'Db2 delimited identifier has unescaped " : {value!r}',
    )  # _validate_identifier: delimited bad
    DB2_14 = ErrorDef(
        ConnInitException,
        "Db2 delimited identifier must be non-empty: {value!r}",
    )  # _validate_identifier: delimited empty
    DB2_15 = ErrorDef(
        ConnInitException,
        "ordinary Db2 identifier must match [A-Za-z_@#$][A-Za-z0-9_@#$]*: {value!r}",
    )  # _validate_identifier: ordinary bad shape
    DB2_16 = ErrorDef(
        ConnInitException,
        "Db2 table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    DB2_17 = ErrorDef(
        ConnInitException,
        "Db2 table name must be 'table' or 'schema.table': {value!r}",
    )  # _validate_table_fqn: bad arity

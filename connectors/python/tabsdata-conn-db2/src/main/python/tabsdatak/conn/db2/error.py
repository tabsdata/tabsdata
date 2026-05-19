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
        Db2RuntimeException,
        "Db2 source connection failed (uri={uri})",
    )  # _Db2SrcPlugin.read_in: engine / connect failure
    DB2_6 = ErrorDef(
        Db2RuntimeException,
        "Db2 query failed (query_index={query_index})",
    )  # _Db2SrcPlugin: read failure inside read_in
    DB2_7 = ErrorDef(
        Db2RuntimeException,
        "Db2Dest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _Db2DestPlugin: defensive recheck
    DB2_8 = ErrorDef(
        Db2RuntimeException,
        "Db2 destination connection failed (uri={uri})",
    )  # _Db2DestPlugin.write_out: engine / connect failure
    DB2_9 = ErrorDef(
        Db2RuntimeException,
        "Db2 write failed (table={table})",
    )  # _Db2DestPlugin: write failure inside write_out

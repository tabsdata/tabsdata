from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class PostgresValidateException(ConnException):
    """Cross-field validation failure for the Postgres connector."""


class PostgresRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Postgres connector."""


class PostgresErrorCode(ErrorCode):
    POSTGRES_1 = ErrorDef(
        ConnInitException,
        "PostgresConn validation failed",
    )  # PostgresConn
    POSTGRES_2 = ErrorDef(
        ConnInitException,
        "PostgresSrc validation failed",
    )  # PostgresSrc
    POSTGRES_3 = ErrorDef(
        PostgresValidateException,
        "PostgresSrc.schema_overrides length {overrides} must equal queries"
        " length {queries}",
    )  # PostgresSrc.validate()
    POSTGRES_4 = ErrorDef(
        ConnInitException,
        "PostgresDest validation failed",
    )  # PostgresDest
    POSTGRES_5 = ErrorDef(
        PostgresRuntimeException,
        "Postgres source connection failed (uri={uri})",
    )  # _PostgresSrcPlugin.read_in: engine / connect failure
    POSTGRES_6 = ErrorDef(
        PostgresRuntimeException,
        "Postgres query failed (query_index={query_index})",
    )  # _PostgresSrcPlugin: read failure inside read_in
    POSTGRES_7 = ErrorDef(
        PostgresRuntimeException,
        "PostgresDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _PostgresDestPlugin: defensive recheck
    POSTGRES_8 = ErrorDef(
        PostgresRuntimeException,
        "Postgres destination connection failed (uri={uri})",
    )  # _PostgresDestPlugin.write_out: engine / connect failure
    POSTGRES_9 = ErrorDef(
        PostgresRuntimeException,
        "Postgres write failed (table={table})",
    )  # _PostgresDestPlugin: write failure inside write_out

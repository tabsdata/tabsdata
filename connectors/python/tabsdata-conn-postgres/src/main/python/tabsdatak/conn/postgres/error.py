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
        ConnInitException,
        "Postgres identifier must be non-empty",
    )  # _validate_identifier: empty
    POSTGRES_6 = ErrorDef(
        ConnInitException,
        "Postgres URI must start with postgres:// or postgresql:// (optionally"
        " driver-qualified, e.g. postgresql+psycopg2://): {value!r}",
    )  # _validate_uri
    POSTGRES_7 = ErrorDef(
        PostgresRuntimeException,
        "Postgres source connection failed (uri={uri})",
    )  # _PostgresSrcPlugin.read_in: engine / connect failure
    POSTGRES_8 = ErrorDef(
        PostgresRuntimeException,
        "Postgres query failed (query_index={query_index})",
    )  # _PostgresSrcPlugin: read failure inside read_in
    POSTGRES_9 = ErrorDef(
        PostgresRuntimeException,
        "Postgres write failed (table={table})",
    )  # _PostgresDestPlugin: write failure inside write_out
    POSTGRES_10 = ErrorDef(
        PostgresRuntimeException,
        "PostgresDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _PostgresDestPlugin: defensive recheck
    POSTGRES_11 = ErrorDef(
        PostgresRuntimeException,
        "Postgres destination connection failed (uri={uri})",
    )  # _PostgresDestPlugin.write_out: engine / connect failure
    POSTGRES_12 = ErrorDef(
        ConnInitException,
        "Postgres identifier exceeds {max} bytes: {value!r}",
    )  # _validate_identifier: too long
    POSTGRES_13 = ErrorDef(
        ConnInitException,
        "double-quoted Postgres identifier may not contain null or"
        ' unescaped " : {value!r}',
    )  # _validate_identifier: double-quoted bad
    POSTGRES_14 = ErrorDef(
        ConnInitException,
        "double-quoted Postgres identifier must be non-empty: {value!r}",
    )  # _validate_identifier: double-quoted empty
    POSTGRES_15 = ErrorDef(
        ConnInitException,
        "unquoted Postgres identifier must match [A-Za-z_][A-Za-z0-9_$]*: {value!r}",
    )  # _validate_identifier: unquoted bad shape
    POSTGRES_16 = ErrorDef(
        ConnInitException,
        "Postgres table name parse failed: {value!r}",
    )  # _validate_table_fqn: split_fqn raised
    POSTGRES_17 = ErrorDef(
        ConnInitException,
        "Postgres table name must be one of 'table', 'schema.table': {value!r}",
    )  # _validate_table_fqn: bad arity

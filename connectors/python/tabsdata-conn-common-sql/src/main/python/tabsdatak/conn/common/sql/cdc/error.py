from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import ErrorCode, ErrorDef


class SqlCdcRuntimeException(ConnException):
    """Runtime failure inside the shared SQL-CDC machinery (table
    discovery, schema introspection, ingester contract violations,
    offset framing). Per-database connectors translate this to their
    own error codes when a more specific signal is useful."""


class SqlCdcInitException(ConnException):
    """Init-time / validation failure for SQL-CDC types
    (``CdcFormat``, ``TimestampPosition``, etc.). Surfaces as
    ``ApiException`` via the framework's dataclass wrappers."""


class SqlCdcErrorCode(ErrorCode):
    # gather_table_identity: the connector's `tables` entry failed to
    # parse into a (schema, table) pair.
    SQL_CDC_1 = ErrorDef(
        SqlCdcRuntimeException,
        "Cannot parse qualified table spec {table!r}: {reason}",
    )
    # gather_table_identity: the parsed schema.table does not exist
    # on the connected DB at startup.
    SQL_CDC_2 = ErrorDef(
        SqlCdcRuntimeException,
        "Table {schema!r}.{table!r} not found on the source database",
    )
    # gather_table_structure: SQLAlchemy returned zero columns for
    # the table -- likely a permission or filtering problem.
    SQL_CDC_3 = ErrorDef(
        SqlCdcRuntimeException,
        "Table {schema!r}.{table!r} has no readable columns",
    )
    # gather_table_structure: the dialect's catalog column count
    # disagrees with SQLAlchemy's introspection count. Suggests a
    # filtered catalog view or a permission issue.
    SQL_CDC_4 = ErrorDef(
        SqlCdcRuntimeException,
        "Table {schema!r}.{table!r}: catalog reports {catalog} column(s) "
        "but SQLAlchemy reports {sa}",
    )
    # gather_table_structure: catalog and SQLAlchemy disagree on
    # column ordering / naming.
    SQL_CDC_5 = ErrorDef(
        SqlCdcRuntimeException,
        "Table {schema!r}.{table!r}: catalog column {catalog!r} does not "
        "match SQLAlchemy column {sa!r}",
    )
    # CdcFormat / position dataclasses: pydantic validation surfaced.
    SQL_CDC_6 = ErrorDef(
        SqlCdcInitException,
        "Cannot create CdcFormat",
    )
    SQL_CDC_7 = ErrorDef(
        SqlCdcInitException,
        "Cannot create TimestampPosition",
    )

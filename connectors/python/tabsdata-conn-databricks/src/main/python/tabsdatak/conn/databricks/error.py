from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class DatabricksValidateException(ConnException):
    """Cross-field validation failure for the Databricks connector."""


class DatabricksRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Databricks connector."""


class DatabricksErrorCode(ErrorCode):
    DATABRICKS_1 = ErrorDef(
        ConnInitException,
        "Databricks connection validation failed",
    )  # DatabricksSrcConn / DatabricksDestConn
    DATABRICKS_2 = ErrorDef(
        DatabricksValidateException,
        "Databricks{cls}: {rule}",
    )  # DatabricksSrcConn / DatabricksDestConn validate()
    DATABRICKS_3 = ErrorDef(
        ConnInitException,
        "DatabricksSrc validation failed",
    )  # DatabricksSrc
    DATABRICKS_4 = ErrorDef(
        ConnInitException,
        "DatabricksDest validation failed",
    )  # DatabricksDest
    DATABRICKS_5 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks source connection failed (host={host})",
    )  # _DatabricksSrcPlugin.read_in: workspace client / connect failure
    DATABRICKS_6 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks query failed (query_index={query_index})",
    )  # _DatabricksSrcPlugin.read_in: query / download failure
    DATABRICKS_7 = ErrorDef(
        DatabricksRuntimeException,
        "DatabricksDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _DatabricksDestPlugin.write_out: defensive recheck
    DATABRICKS_8 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks destination connection failed (host={host})",
    )  # _DatabricksDestPlugin.write_out: workspace / sql connect failure
    DATABRICKS_9 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks COPY INTO failed (table={table})",
    )  # _DatabricksDestPlugin.write_out: COPY INTO failure
    DATABRICKS_10 = ErrorDef(
        DatabricksValidateException,
        "Databricks table name {value!r} must resolve to a fully-"
        "qualified 'catalog.schema.table' (set conn.catalog and/or"
        " conn.schema, or use a 3-part name)",
    )  # _writer._fully_qualify_table
    DATABRICKS_11 = ErrorDef(
        DatabricksValidateException,
        "Databricks initial_values scratch entry must be a "
        "dict[str, str] (got {actual_type}). Set with "
        "ctx.store_attr(DATABRICKS_INITIAL_VALUES_KEY, "
        "{{...}}).",
    )  # _resolve_initial_values: non-dict value under the connector's key

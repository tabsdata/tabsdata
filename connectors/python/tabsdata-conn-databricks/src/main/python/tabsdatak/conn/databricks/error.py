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


class DatabricksValidateException(ConnException):
    """Cross-field validation failure for the Databricks connector.

    :categories: error
    """


class DatabricksRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the Databricks connector.

    :categories: error
    """


class DatabricksErrorCode(ErrorCode):
    """Error codes for the Databricks connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `DatabricksErrorCode.DATABRICKS_N.exception`.
    """

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
        DatabricksRuntimeException,
        "Databricks query returned no rows and no result schema was available"
        " to materialize an empty result (query_index={query_index})",
    )  # execute_query: empty result with no manifest schema
    DATABRICKS_12 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks rejected the query: it binds {parameter}, which"
        " initial_values does not provide",
    )  # _DatabricksSrcPlugin.read_in: UNBOUND_SQL_PARAMETER from the warehouse
    DATABRICKS_13 = ErrorDef(
        DatabricksRuntimeException,
        "Databricks has no time-of-day type, so table {table!r} cannot take"
        " the column(s) {columns}. Cast them before writing -- for example to"
        " a timestamp or a string",
    )  # write_all_tables: parquet TIME column, rejected before uploading

    # --- Field-value rules (tabsdatak.conn.databricks) --------------
    DATABRICKS_14 = ErrorDef(
        ConnInitException,
        "not a valid Databricks identifier (up to 255 chars, letters, digits, "
        "underscore and hyphen, or a backtick-quoted name): {value!r}",
    )  # IdentifierSpec
    DATABRICKS_15 = ErrorDef(
        ConnInitException,
        "not a valid Databricks warehouse name (1-100 chars, no leading or "
        "trailing whitespace, no control characters): {value!r}",
    )  # WarehouseSpec
    DATABRICKS_16 = ErrorDef(
        ConnInitException,
        "not a valid Databricks warehouse id (16 lowercase hex chars): {value!r}",
    )  # WarehouseIdSpec
    DATABRICKS_17 = ErrorDef(
        ConnInitException,
        "not a valid Databricks qualified name (1 to 3 dot-separated identifier "
        "parts, e.g. 'catalog.schema.volume'): {value!r}",
    )  # VolumeSpec

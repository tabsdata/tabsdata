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


class BigQueryValidateException(ConnException):
    """Cross-field validation failure for the BigQuery connector.

    :categories: error
    """


class BigQueryRuntimeException(ConnException):
    """Runtime failure (plugin / I/O) for the BigQuery connector.

    :categories: error
    """


class BigQueryErrorCode(ErrorCode):
    """Error codes for the BigQuery connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `BigQueryErrorCode.BIGQUERY_N.exception`.
    """

    BIGQUERY_1 = ErrorDef(
        ConnInitException,
        "BigQueryDestConn validation failed",
    )  # BigQueryDestConn
    BIGQUERY_2 = ErrorDef(
        BigQueryValidateException,
        "BigQueryDestConn.dataset={dataset!r} requires project to be set",
    )  # BigQueryDestConn.validate()
    BIGQUERY_3 = ErrorDef(
        ConnInitException,
        "BigQueryDest validation failed",
    )  # BigQueryDest
    BIGQUERY_4 = ErrorDef(
        BigQueryRuntimeException,
        "BigQueryDest tables/slot count mismatch (tables={tables}, slots={slots})",
    )  # _BigQueryDestPlugin: defensive recheck
    BIGQUERY_5 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery client creation failed (project={project})",
    )  # _BigQueryDestPlugin: client construction failure
    BIGQUERY_6 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery staging upload to GCS failed (uri={uri})",
    )  # _BigQueryDestPlugin: GCS staging upload failure
    BIGQUERY_7 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery load job failed (table={table})",
    )  # _BigQueryDestPlugin: load_table_from_uri failure
    BIGQUERY_8 = ErrorDef(
        ConnInitException,
        "Cannot create GCSLocation",
    )  # GCSLocation

    # --- Field-value rules (tabsdatak.conn.bigquery) ----------------
    BIGQUERY_9 = ErrorDef(
        ConnInitException,
        "not a valid BigQuery project id (6-30 chars, lowercase letters, digits "
        "and hyphens, starting with a letter and ending alphanumeric): {value!r}",
    )  # ProjectSpec
    BIGQUERY_10 = ErrorDef(
        ConnInitException,
        "not a valid BigQuery dataset id (1-1024 chars, ASCII letters, digits "
        "and underscores): {value!r}",
    )  # DatasetSpec

    # --- Merge (tabsdatak.conn.bigquery) ----------------------------
    BIGQUERY_11 = ErrorDef(
        ConnInitException,
        "MergeCfg validation failed",
    )  # MergeCfg
    BIGQUERY_12 = ErrorDef(
        BigQueryValidateException,
        'BigQueryDest.if_table_exists="merge" requires merge_cfg to be set',
    )  # BigQueryDest.validate()
    BIGQUERY_13 = ErrorDef(
        BigQueryValidateException,
        "BigQueryDest.merge_cfg is set but if_table_exists={if_table_exists!r};"
        ' merge_cfg only applies to if_table_exists="merge"',
    )  # BigQueryDest.validate()
    BIGQUERY_14 = ErrorDef(
        BigQueryValidateException,
        "BigQueryDest.merge_cfg has {cfgs} entries but tables has {tables}; a"
        " list of merge configs must line up one-to-one with tables (pass a"
        " single MergeCfg to apply one config to every table)",
    )  # BigQueryDest.validate()
    BIGQUERY_15 = ErrorDef(
        BigQueryRuntimeException,
        "MergeCfg.op_column={column!r} is not a column of the data written to"
        " table {table}, so the merge cannot read each row's i/u/d marker. Every"
        " incoming row has to carry it",
    )  # _written_columns: op column absent from the incoming data
    BIGQUERY_16 = ErrorDef(
        BigQueryRuntimeException,
        "MergeCfg.on names {columns}, which the data written to table {table}"
        " does not carry, so incoming rows cannot be matched against it. Every"
        " key column has to be a column of the frame",
    )  # _written_columns: key column absent from the incoming data
    BIGQUERY_17 = ErrorDef(
        BigQueryRuntimeException,
        "MergeCfg.on names {columns}, which table {table} does not have, so its"
        " rows cannot be matched against the incoming ones. Every key column has"
        " to be a column of the target too",
    )  # _written_columns: key column absent from the target table
    BIGQUERY_18 = ErrorDef(
        BigQueryRuntimeException,
        "BigQuery merge onto table {table} failed",
    )  # _BigQueryDestPlugin.write_out: schema evolution / MERGE failure
    BIGQUERY_19 = ErrorDef(
        BigQueryRuntimeException,
        "MergeCfg column {column!r} matches {matches} of the data written to"
        " table {table}. BigQuery resolves column names case-insensitively and"
        " refuses a table two columns whose names differ only in case, so they"
        " cannot be told apart",
    )  # _incoming_column: ambiguous case-insensitive match
    BIGQUERY_20 = ErrorDef(
        BigQueryRuntimeException,
        "Table {table} shares no column with the incoming data, so a merge onto"
        " it would write nothing. Check the target name and that the frame's"
        " columns match the table's",
    )  # the merge driver: nothing to write

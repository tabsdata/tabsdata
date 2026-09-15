#
# Copyright 2026 Tabsdata Inc.
#

"""Error machinery for the shared CDC connector code.

Mirrors the per-connector `error.py` convention (see
`tabsdatak.conn.common.sql.error`): a single `ErrorCode` enum whose
variants each carry an `ErrorDef` (exception class + message
template). Raise via `SqlCdcErrorCode.CDC_N.exception(...)`, e.g.
`SqlCdcErrorCode.CDC_2.exception(schema=schema, table=table)`.

Argument validation bound to one of these codes goes through
`tabsdatak._api.validate_call`.
"""

from __future__ import annotations

from tabsdatak.conn.common.error import ConnException
from tabsdatak.error import ErrorCode, ErrorDef


class SqlCdcException(ConnException):
    """Failure raised by the shared CDC machinery.

    :categories: error

    Covers table resolution, schema reflection, buffering / offset
    handling and spec validation. Connector-specific CDC code can
    translate it into its own per-connector codes when relevant.
    """


class SqlCdcErrorCode(ErrorCode):
    """Error codes for the shared CDC machinery.

    :categories: error

    One variant per validation / runtime site. Raise via
    `SqlCdcErrorCode.CDC_N.exception`.
    """

    CDC_1 = ErrorDef(
        SqlCdcException,
        "Invalid table specification {spec!r}",
    )
    CDC_2 = ErrorDef(
        SqlCdcException,
        "Table {schema}.{table} not found in database",
    )
    CDC_3 = ErrorDef(
        SqlCdcException,
        "Table {schema}.{table} has no columns",
    )
    CDC_4 = ErrorDef(
        SqlCdcException,
        "CD table {schema}.{table} not found in ASN registration table "
        "{registry}.ibmsnap_register. Refer to the documentation for "
        "instructions on configuring Db2 table replication using ASN, or "
        "contact your system administrator to configure it.",
    )
    CDC_5 = ErrorDef(
        SqlCdcException,
        "Unknown CDC operation type {operation!r} from {source} in table {table}",
    )
    CDC_6 = ErrorDef(
        SqlCdcException,
        "Unsupported start_from value {value!r}",
    )
    CDC_7 = ErrorDef(
        SqlCdcException,
        "Column count mismatch for table {schema}.{table}: catalog returned "
        "{catalog_count} columns but inspector returned {inspector_count}",
    )
    CDC_8 = ErrorDef(
        SqlCdcException,
        "Column name mismatch for table {schema}.{table}: catalog column "
        "{catalog_column!r} does not match inspector column {inspector_column!r}",
    )
    CDC_9 = ErrorDef(
        SqlCdcException,
        "Staged file not found for {file!r} in table {table!r}",
    )
    CDC_10 = ErrorDef(
        SqlCdcException,
        "Buffer sentinel has no base offset; ingester must provide a "
        "base offset at initialization",
    )
    CDC_11 = ErrorDef(
        SqlCdcException,
        "Received change with no offset for table {table!r}",
    )
    CDC_12 = ErrorDef(
        SqlCdcException,
        "Expected offset type {expected!r} but received {actual!r}",
    )
    CDC_13 = ErrorDef(
        SqlCdcException,
        "Invalid CDC timestamp position specification",
    )
    CDC_14 = ErrorDef(
        SqlCdcException,
        "Invalid CDC format specification",
    )
    CDC_15 = ErrorDef(
        SqlCdcException,
        "Failed to process staged CDC files for output slot {index}: {reason}",
    )
    CDC_16 = ErrorDef(
        SqlCdcException,
        "coalesce_cdc_batch needs at least one key column: it is what change "
        "records are grouped by, and what the merge matches rows on",
    )  # coalesce_cdc_batch
    CDC_17 = ErrorDef(
        SqlCdcException,
        "coalesce_cdc_batch op_column={column!r} is also a key column; a key "
        "identifies the row, so it cannot double as the row's change marker",
    )  # coalesce_cdc_batch
    CDC_18 = ErrorDef(
        SqlCdcException,
        "coalesce_cdc_batch order_column={column!r} is also a key column; it "
        "holds one value per key group, so it cannot order the records within "
        "a group",
    )  # coalesce_cdc_batch
    CDC_19 = ErrorDef(
        SqlCdcException,
        "coalesce_cdc_batch order_column={column!r} is also the op_column; the "
        "arrival order of a key's records cannot be read from the change marker",
    )  # coalesce_cdc_batch

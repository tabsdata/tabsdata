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
    CDC_20 = ErrorDef(
        SqlCdcException,
        "Value of large text column {column!r} in table {table!r} has {length} "
        "characters, above the tabsdata.cdc.max_large_text_length limit of "
        "{limit}",
    )
    CDC_21 = ErrorDef(
        SqlCdcException,
        "An initial load requires the resume offset to be kept in Tabsdata "
        "stream state, but {ingester} commits it on the source",
    )
    CDC_22 = ErrorDef(
        SqlCdcException,
        "Primary key column {column!r} of table {table!r} is not in the schema of "
        "the changes committed during its initial load. Run the initial load "
        "again",
    )  # deduplicate_initial_load
    CDC_23 = ErrorDef(
        SqlCdcException,
        "Key {key} of table {table!r} would hold two rows while applying the "
        "changes committed during its initial load: a key swap, or a row the "
        "load read before and after its key changed. Run the initial load "
        "again at a time without key changes",
    )  # deduplicate_initial_load
    CDC_24 = ErrorDef(
        SqlCdcException,
        "Table {table!r} was truncated during its initial load. Run the initial "
        "load again",
    )  # deduplicate_initial_load
    CDC_25 = ErrorDef(
        SqlCdcException,
        "The schema of table {table!r} changed during its initial load. Run the "
        "initial load again",
    )  # deduplicate_initial_load
    CDC_26 = ErrorDef(
        SqlCdcException,
        "An initial load cannot be deduplicated with columns_format='map', whose "
        "values carry no types to compare by. Use 'columns' or 'struct'",
    )  # deduplicate_initial_load
    CDC_27 = ErrorDef(
        SqlCdcException,
        "Change operation {op!r} of table {table!r} is not one the initial-load "
        "deduplicator handles",
    )  # deduplicate_initial_load
    CDC_28 = ErrorDef(
        SqlCdcException,
        "The changes of table {table!r} committed during its initial load are not "
        "in the order the stream delivered them: position {position} comes after "
        "{previous}",
    )  # deduplicate_initial_load
    CDC_29 = ErrorDef(
        SqlCdcException,
        "More than {limit} changes were committed to the tracked tables while the "
        "initial load read them (tabsdata.cdc.initial_load.max_window_changes). "
        "Run the initial load again at a time with fewer changes, or raise the "
        "limit",
    )  # _read_window
    CDC_30 = ErrorDef(
        SqlCdcException,
        "{ingester} reported a position to record without staging, but gave "
        "{offset!r}: it must report the offset the stream resumes from, and "
        "keep that offset in Tabsdata stream state",
    )  # _checkpoint

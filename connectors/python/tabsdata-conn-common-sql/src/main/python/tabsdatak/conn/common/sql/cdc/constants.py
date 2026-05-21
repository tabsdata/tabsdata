"""Constants shared by every SQL-CDC connector.

Names mirror the legacy ``tabsdata.connector.cdc.common.constants``
module so column-name conventions on the wire stay identical and
existing downstream pipelines continue to recognise them.
"""

from __future__ import annotations

from enum import Enum

# Reserved column-name prefix that marks framework-emitted columns
# (CDC metadata, soft-delete flags, etc.). Mirrors the legacy value
# from `tabsdata._utils.tableframe._constants.TD_CONNECTOR_COLUMN_PREFIX`.
TD_CONNECTOR_COLUMN_PREFIX = "@td."


class CdcOperation(str, Enum):
    INSERT = "i"
    UPDATE = "u"
    DELETE = "d"


OP_INSERT = CdcOperation.INSERT
OP_UPDATE = CdcOperation.UPDATE
OP_DELETE = CdcOperation.DELETE


# --- Column-name infixes ----------------------------------------------

TD_CDC_INFIX = "cdc"
TD_CDC_META_INFIX = "meta"
TD_CDC_DATA_INFIX = "data"

TD_CDC_COL_INFIX = "col"
TD_CDC_ROW_INFIX = "row"
TD_CDC_MAP_INFIX = "map"

TD_CDC_OLD_INFIX = "old"
TD_CDC_NEW_INFIX = "new"

TD_CDC_OP_INFIX = "op"
TD_CDC_TX_INFIX = "tx"
TD_CDC_SQ_INFIX = "sq"
TD_CDC_TS_INFIX = "ts"
TD_CDC_FMT_INFIX = "fmt"
TD_CDC_FLAT_INFIX = "flat"
TD_CDC_SCH_INFIX = "sch"

DOT = "."
COMMA = ","


# --- Composite meta-column names --------------------------------------

CDC_META_PREFIX = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_META_INFIX,
])

CDC_META_OP = DOT.join([CDC_META_PREFIX, TD_CDC_OP_INFIX])
CDC_META_TX = DOT.join([CDC_META_PREFIX, TD_CDC_TX_INFIX])
CDC_META_SQ = DOT.join([CDC_META_PREFIX, TD_CDC_SQ_INFIX])
CDC_META_TS = DOT.join([CDC_META_PREFIX, TD_CDC_TS_INFIX])
CDC_META_FMT = DOT.join([CDC_META_PREFIX, TD_CDC_FMT_INFIX])
CDC_META_FLAT = DOT.join([CDC_META_PREFIX, TD_CDC_FLAT_INFIX])
CDC_META_SCH = DOT.join([CDC_META_PREFIX, TD_CDC_SCH_INFIX])


# --- Composite data-column prefixes -----------------------------------

CDC_DATA_COL_PREFIX = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_COL_INFIX,
])

CDC_DATA_COL_OLD_PREFIX = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_COL_INFIX,
    TD_CDC_OLD_INFIX,
])

CDC_DATA_COL_NEW_PREFIX = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_COL_INFIX,
    TD_CDC_NEW_INFIX,
])

CDC_DATA_ROW_OLD = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_ROW_INFIX,
    TD_CDC_OLD_INFIX,
])

CDC_DATA_ROW_NEW = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_ROW_INFIX,
    TD_CDC_NEW_INFIX,
])

CDC_DATA_MAP_OLD = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_MAP_INFIX,
    TD_CDC_OLD_INFIX,
])

CDC_DATA_MAP_NEW = DOT.join([
    TD_CONNECTOR_COLUMN_PREFIX,
    TD_CDC_INFIX,
    TD_CDC_DATA_INFIX,
    TD_CDC_MAP_INFIX,
    TD_CDC_NEW_INFIX,
])


# --- Runner / sentinel defaults ---------------------------------------

# In-memory buffer thresholds (rows / bytes / seconds) before a flush
# to the working directory. ``None`` disables the corresponding cap.
DEFAULT_BUFFER_MAX_ROWS = 10_000
DEFAULT_BUFFER_MAX_BYTES: int | None = None
DEFAULT_BUFFER_MAX_SECS = 60

# Working-dir → stage thresholds. ``None`` disables the corresponding
# cap. The disk sentinel triggers a manifest stage call when any cap
# is hit.
DEFAULT_TRIGGER_MAX_ROWS: int | None = None
DEFAULT_TRIGGER_MAX_BYTES: int | None = None
DEFAULT_TRIGGER_MAX_SECS = 60

# Polling cadence and how long the ingester blocks waiting for the
# next event from the source's change stream.
DEFAULT_POLL_INTERVAL_SECONDS = 1
DEFAULT_BLOCKING_TIMEOUT_SECONDS = 1

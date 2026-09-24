#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from enum import Enum


class CdcOperation(str, Enum):
    """The change operation carried by an emitted CDC row.

    :categories: source

    Recorded in the change metadata of each row a CDC source emits.
    Row operations carry a row image: `INSERT` (`"i"`), `UPDATE`
    (`"u"`) or `DELETE` (`"d"`). Control operations carry no row image
    (every data column is null) and tell the consumer what happened to
    the table as a whole: `LOAD` (`"l"`) opens an initial load, so the
    destination must be emptied before the rows that follow are applied;
    `TRUNCATE` (`"t"`) reports the source table was truncated.
    """

    INSERT = "i"
    UPDATE = "u"
    DELETE = "d"
    LOAD = "l"
    TRUNCATE = "t"


OP_INSERT = CdcOperation.INSERT
OP_UPDATE = CdcOperation.UPDATE
OP_DELETE = CdcOperation.DELETE
OP_LOAD = CdcOperation.LOAD
OP_TRUNCATE = CdcOperation.TRUNCATE

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
TD_CDC_SEG_INFIX = "seg"
TD_CDC_ORD_INFIX = "ord"
TD_CDC_FMT_INFIX = "fmt"
TD_CDC_FLAT_INFIX = "flat"
TD_CDC_SCH_INFIX = "sch"

DOT = "."
COMMA = ","

CDC_META_PREFIX = "@td.cdc.meta"

CDC_META_OP = "@td.cdc.meta.op"
CDC_META_TX = "@td.cdc.meta.tx"
CDC_META_SQ = "@td.cdc.meta.sq"
CDC_META_TS = "@td.cdc.meta.ts"
CDC_META_SEG = "@td.cdc.meta.seg"
CDC_META_ORD = "@td.cdc.meta.ord"
CDC_META_FMT = "@td.cdc.meta.fmt"
CDC_META_FLAT = "@td.cdc.meta.flat"
CDC_META_SCH = "@td.cdc.meta.sch"

CDC_DATA_COL_PREFIX = "@td.cdc.data.col"

CDC_DATA_COL_OLD_PREFIX = "@td.cdc.data.col.old"
CDC_DATA_COL_NEW_PREFIX = "@td.cdc.data.col.new"

CDC_DATA_ROW_OLD = "@td.cdc.data.row.old"
CDC_DATA_ROW_NEW = "@td.cdc.data.row.new"

CDC_DATA_MAP_OLD = "@td.cdc.data.map.old"
CDC_DATA_MAP_NEW = "@td.cdc.data.map.new"

DEFAULT_BUFFER_MAX_ROWS = 10_000
DEFAULT_BUFFER_MAX_BYTES = None
DEFAULT_BUFFER_MAX_SECS = 60

DEFAULT_TRIGGER_MAX_ROWS = None
DEFAULT_TRIGGER_MAX_BYTES = None
DEFAULT_TRIGGER_MAX_SECS = 60

DEFAULT_POLL_INTERVAL_SECONDS = 1
DEFAULT_BLOCKING_TIMEOUT_SECONDS = 1

DEFAULT_MAX_LARGE_TEXT_LENGTH = 1024

CDC_INITIAL_LOAD_TX = "initial-load"

DEFAULT_FETCH_SIZE = 100_000

DEFAULT_CHUNK_SIZE = 100_000

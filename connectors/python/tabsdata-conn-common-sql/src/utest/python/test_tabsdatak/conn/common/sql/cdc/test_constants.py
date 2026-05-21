"""Wire-shape constants -- column-name composites and default
thresholds. Asserts the column-name string layout because downstream
consumers parse on these literals."""

from __future__ import annotations

from tabsdatak.conn.common.sql.cdc import constants as C


def test_cdc_operation_codes():
    assert C.CdcOperation.INSERT == "i"
    assert C.CdcOperation.UPDATE == "u"
    assert C.CdcOperation.DELETE == "d"
    assert C.OP_INSERT is C.CdcOperation.INSERT


def test_meta_column_names_use_td_prefix():
    # ``TD_CONNECTOR_COLUMN_PREFIX`` includes its trailing dot
    # (``"@td."``), so the ``DOT.join`` composition produces a
    # double-dot in the final name. The legacy CDC connectors emit
    # the same shape; downstream consumers parse on these literals.
    assert C.CDC_META_OP == "@td..cdc.meta.op"
    assert C.CDC_META_TX == "@td..cdc.meta.tx"
    assert C.CDC_META_SQ == "@td..cdc.meta.sq"
    assert C.CDC_META_FMT == "@td..cdc.meta.fmt"
    assert C.CDC_META_FLAT == "@td..cdc.meta.flat"
    assert C.CDC_META_SCH == "@td..cdc.meta.sch"


def test_data_column_prefixes():
    assert C.CDC_DATA_COL_NEW_PREFIX == "@td..cdc.data.col.new"
    assert C.CDC_DATA_COL_OLD_PREFIX == "@td..cdc.data.col.old"
    assert C.CDC_DATA_ROW_NEW == "@td..cdc.data.row.new"
    assert C.CDC_DATA_ROW_OLD == "@td..cdc.data.row.old"
    assert C.CDC_DATA_MAP_NEW == "@td..cdc.data.map.new"
    assert C.CDC_DATA_MAP_OLD == "@td..cdc.data.map.old"


def test_default_buffer_thresholds():
    assert C.DEFAULT_BUFFER_MAX_ROWS == 10_000
    assert C.DEFAULT_BUFFER_MAX_BYTES is None
    assert C.DEFAULT_BUFFER_MAX_SECS == 60


def test_default_trigger_thresholds():
    assert C.DEFAULT_TRIGGER_MAX_ROWS is None
    assert C.DEFAULT_TRIGGER_MAX_BYTES is None
    assert C.DEFAULT_TRIGGER_MAX_SECS == 60


def test_default_polling():
    assert C.DEFAULT_POLL_INTERVAL_SECONDS == 1
    assert C.DEFAULT_BLOCKING_TIMEOUT_SECONDS == 1

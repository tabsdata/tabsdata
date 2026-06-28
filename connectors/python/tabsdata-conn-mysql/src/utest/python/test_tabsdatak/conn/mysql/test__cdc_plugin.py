"""``MySQLCdcStreamSrcPlugin`` -- assert the StreamSrcDef shape
and that ``run`` wires a BinlogIngester + run_cdc with the right
config translated from the user-facing ``src_cfg``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc import CdcStageSrc
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import (
    MySQLCdcStreamSrcConn,
    MySQLCdcStreamSrc,
)
from tabsdatak.conn.mysql._cdc_plugin import (
    MYSQL_CDC_STREAM_SRC,
    MySQLCdcStreamSrcPlugin,
    _build_cdc_run_config,
    _to_internal_start_from,
)


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _conn() -> MySQLCdcStreamSrcConn:
    return MySQLCdcStreamSrcConn(
        uri="mysql://h:3306/d",
        credentials=_creds(),
        server_id=777,
    )


def _src(**overrides) -> MySQLCdcStreamSrc:
    kw = dict(
        tables=["app.users", "app.orders"],
        start_from="tail",
    )
    kw.update(overrides)
    return MySQLCdcStreamSrc(**kw)


# --- StreamSrcDef shape ---------------------------------------------


def test_stream_src_def_is_for_mysql_cdc():
    assert MYSQL_CDC_STREAM_SRC.conn is MySQLCdcStreamSrcConn
    assert MYSQL_CDC_STREAM_SRC.src is MySQLCdcStreamSrc
    assert MYSQL_CDC_STREAM_SRC.plugin is MySQLCdcStreamSrcPlugin
    assert MYSQL_CDC_STREAM_SRC.src_type == "mysql-cdc"
    assert MYSQL_CDC_STREAM_SRC.publisher_src is CdcStageSrc


def test_stream_src_def_cardinality_matches_table_count():
    src = _src(tables=["a.t1", "a.t2", "a.t3"])
    assert MYSQL_CDC_STREAM_SRC.cardinality(src) == 3


# --- _build_cdc_run_config from src_cfg -----------------------------


def test_run_config_uses_defaults_when_no_src_cfg():
    src = _src()
    cfg = _build_cdc_run_config(src)
    # Defaults come from tabsdatak.conn.common.sql.cdc.constants.
    assert isinstance(cfg, CdcRunConfig)
    assert cfg.buffer_max_rows == 10_000
    assert cfg.buffer_max_secs == 60
    assert cfg.poll_interval_seconds == 1


def test_run_config_picks_up_src_cfg_overrides():
    src = _src(
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 5,
            "tabsdata.cdc.buffer.max_secs": 30,
            "tabsdata.cdc.poll_interval_seconds": 2,
        },
    )
    cfg = _build_cdc_run_config(src)
    assert cfg.buffer_max_rows == 5
    assert cfg.buffer_max_secs == 30
    assert cfg.poll_interval_seconds == 2


def test_run_config_carries_format_fields_from_src():
    from tabsdatak.conn.common.sql.cdc.typing import (
        CdcFormat as _CommonCdcFormat,
    )

    src = _src(columns_format="struct", flatten_columns=False)
    cfg = _build_cdc_run_config(src)
    # The runner sees a *common* CdcFormat composed by the plugin
    # from the two flat fields on MySQLCdcStreamSrc.
    assert isinstance(cfg.cdc_format, _CommonCdcFormat)
    assert cfg.cdc_format.columns_format == "struct"
    assert cfg.cdc_format.flatten_columns is False


def test_run_config_format_fields_default_to_columns_flattened():
    src = _src()  # columns_format and flatten_columns defaulted
    cfg = _build_cdc_run_config(src)
    assert cfg.cdc_format.columns_format == "columns"
    assert cfg.cdc_format.flatten_columns is True


# --- logging_level wiring -------------------------------------------


def test_run_config_logging_level_default_is_error():
    import logging
    cfg = _build_cdc_run_config(_src())
    assert cfg.logging_level == logging.ERROR


def test_run_config_logging_level_picks_up_src_cfg_override():
    import logging
    src = _src(src_cfg={"mysql.cdc.logging_level": logging.DEBUG})
    cfg = _build_cdc_run_config(src)
    assert cfg.logging_level == logging.DEBUG


def test_plugin_run_applies_logging_level_to_connector_logger():
    import logging
    src = _src(src_cfg={"mysql.cdc.logging_level": logging.DEBUG})
    ctx = MagicMock()
    connector_logger = logging.getLogger("tabsdatak.conn.mysql._cdc_plugin")
    connector_logger.setLevel(logging.NOTSET)
    try:
        # ``run`` is inherited from ``CdcPlugin``; patch the shared loop so
        # ``run`` just does its setup (config + log levels + ingester) and
        # returns.
        with patch(
            "tabsdatak.conn.common.sql.cdc._runner._run_loop",
        ), patch(
            "tabsdatak.conn.mysql._cdc_plugin.BinlogIngester",
        ):
            MySQLCdcStreamSrcPlugin().run(ctx=ctx, conn=_conn(), stream_src=src)
        assert connector_logger.level == logging.DEBUG
    finally:
        connector_logger.setLevel(logging.NOTSET)


# --- build_ingester wires the right BinlogIngester ------------------


def test_build_ingester_constructs_binlog_with_server_id_and_blocking():
    src = _src(
        src_cfg={"mysql.cdc.blocking_timeout_seconds": 7},
    )
    conn = _conn()

    with patch(
        "tabsdatak.conn.mysql._cdc_plugin.BinlogIngester",
    ) as mock_ingester_cls:
        mock_ingester_cls.return_value = "ingester-instance"
        ingester = MySQLCdcStreamSrcPlugin().build_ingester(conn, src)

    mock_ingester_cls.assert_called_once()
    kw = mock_ingester_cls.call_args.kwargs
    assert kw["conn"] is conn
    assert kw["tables"] == ["app.users", "app.orders"]
    assert kw["start_from"] == "tail"
    assert kw["server_id"] == 777
    assert kw["blocking_timeout_seconds"] == 7
    assert ingester == "ingester-instance"


# --- Public -> internal conversion at the plugin boundary -----------


def test_to_internal_start_from_translates_timestamp_position():
    from datetime import datetime, timezone

    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )
    from tabsdatak.conn.mysql import TimestampPosition

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    out = _to_internal_start_from(TimestampPosition(ts=ts))
    assert isinstance(out, _CommonTimestampPosition)
    assert out.ts == ts


def test_to_internal_start_from_passes_through_gtid_and_binlog():
    from tabsdatak.conn.mysql import BinlogPosition, GtidPosition

    g = GtidPosition(gtid="uuid:1")
    b = BinlogPosition(file="bin.000001", pos=4)
    assert _to_internal_start_from(g) is g
    assert _to_internal_start_from(b) is b


def test_to_internal_start_from_passes_through_relative_keywords():
    assert _to_internal_start_from("head") == "head"
    assert _to_internal_start_from("tail") == "tail"


def test_build_ingester_translates_timestamp_start_from():
    from datetime import datetime, timezone

    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )
    from tabsdatak.conn.mysql import TimestampPosition

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    src = _src(start_from=TimestampPosition(ts=ts))

    with patch(
        "tabsdatak.conn.mysql._cdc_plugin.BinlogIngester",
    ) as mock_ingester_cls:
        MySQLCdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    assert isinstance(kw["start_from"], _CommonTimestampPosition)
    assert kw["start_from"].ts == ts


def test_build_ingester_blocking_timeout_falls_back_to_default():
    src = _src()  # no src_cfg

    with patch(
        "tabsdatak.conn.mysql._cdc_plugin.BinlogIngester",
    ) as mock_ingester_cls:
        MySQLCdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_BLOCKING_TIMEOUT_SECONDS from common-sql constants.
    assert kw["blocking_timeout_seconds"] == 1

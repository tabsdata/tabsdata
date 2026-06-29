"""``Db2CdcStreamSrcPlugin`` -- assert the StreamSrcDef shape
and that ``run`` wires a SqlReplicaIngester + run_cdc with the right
config translated from the user-facing ``src_cfg``."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc import CdcStageSrc
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.db2 import (
    CommitSeqPosition,
    Db2CdcStreamSrcConn,
    Db2CdcStreamSrc,
    TableCommitSeqPosition,
    TimestampPosition,
)
from tabsdatak.conn.db2._cdc_plugin import (
    DB2_CDC_STREAM_SRC,
    Db2CdcStreamSrcPlugin,
    _build_cdc_run_config,
    _to_internal_start_from,
)


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _conn() -> Db2CdcStreamSrcConn:
    return Db2CdcStreamSrcConn(
        uri="db2+ibm_db://h:50000/sample",
        credentials=_creds(),
    )


def _src(**overrides) -> Db2CdcStreamSrc:
    kw = dict(
        tables=["CDSCHEMA.USERS_CD", "CDSCHEMA.ORDERS_CD"],
        start_from="tail",
    )
    kw.update(overrides)
    return Db2CdcStreamSrc(**kw)


# --- StreamSrcDef shape ---------------------------------------------


def test_stream_src_def_is_for_db2_cdc():
    assert DB2_CDC_STREAM_SRC.conn is Db2CdcStreamSrcConn
    assert DB2_CDC_STREAM_SRC.src is Db2CdcStreamSrc
    assert DB2_CDC_STREAM_SRC.plugin is Db2CdcStreamSrcPlugin
    assert DB2_CDC_STREAM_SRC.type_ == "db2-cdc"
    assert DB2_CDC_STREAM_SRC.publisher_src is CdcStageSrc


def test_stream_src_def_cardinality_matches_table_count():
    src = _src(tables=["S.T1", "S.T2", "S.T3"])
    assert DB2_CDC_STREAM_SRC.cardinality(src) == 3


# --- _build_cdc_run_config from src_cfg -----------------------------


def test_run_config_uses_defaults_when_no_src_cfg():
    src = _src()
    cfg = _build_cdc_run_config(src)
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
    assert isinstance(cfg.cdc_format, _CommonCdcFormat)
    assert cfg.cdc_format.columns_format == "struct"
    assert cfg.cdc_format.flatten_columns is False


def test_run_config_format_fields_default_to_columns_flattened():
    src = _src()
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
    src = _src(src_cfg={"db2.cdc.logging_level": logging.DEBUG})
    cfg = _build_cdc_run_config(src)
    assert cfg.logging_level == logging.DEBUG


def test_plugin_run_applies_logging_level_to_connector_logger():
    import logging
    src = _src(src_cfg={"db2.cdc.logging_level": logging.DEBUG})
    ctx = MagicMock()
    connector_logger = logging.getLogger("tabsdatak.conn.db2._cdc_plugin")
    connector_logger.setLevel(logging.NOTSET)
    try:
        # ``run`` is inherited from ``CdcPlugin``; patch the shared loop so
        # ``run`` just does its setup (config + log levels + ingester) and
        # returns.
        with patch(
            "tabsdatak.conn.common.sql.cdc._runner._run_loop",
        ), patch(
            "tabsdatak.conn.db2._cdc_plugin.SqlReplicaIngester",
        ):
            Db2CdcStreamSrcPlugin().run(ctx=ctx, conn=_conn(), stream_src=src)
        assert connector_logger.level == logging.DEBUG
    finally:
        connector_logger.setLevel(logging.NOTSET)


# --- build_ingester wires the right SqlReplicaIngester --------------


def test_build_ingester_constructs_with_timeout_and_fetch_size():
    src = _src(
        src_cfg={
            "db2.cdc.blocking_timeout_seconds": 7,
            "db2.cdc.fetch_size": 5000,
        },
    )
    conn = _conn()

    with patch(
        "tabsdatak.conn.db2._cdc_plugin.SqlReplicaIngester",
    ) as mock_ingester_cls:
        mock_ingester_cls.return_value = "ingester-instance"
        ingester = Db2CdcStreamSrcPlugin().build_ingester(conn, src)

    mock_ingester_cls.assert_called_once()
    kw = mock_ingester_cls.call_args.kwargs
    assert kw["conn"] is conn
    assert kw["tables"] == ["CDSCHEMA.USERS_CD", "CDSCHEMA.ORDERS_CD"]
    assert kw["start_from"] == "tail"
    assert kw["blocking_timeout_seconds"] == 7
    assert kw["fetch_size"] == 5000
    assert ingester == "ingester-instance"


def test_build_ingester_defaults_when_no_src_cfg():
    src = _src()

    with patch(
        "tabsdatak.conn.db2._cdc_plugin.SqlReplicaIngester",
    ) as mock_ingester_cls:
        Db2CdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_BLOCKING_TIMEOUT_SECONDS from common-sql constants.
    assert kw["blocking_timeout_seconds"] == 1
    # DEFAULT_FETCH_SIZE from _cdc_impl.
    assert kw["fetch_size"] == 10000


# --- Public -> internal conversion at the plugin boundary -----------


def test_to_internal_start_from_translates_timestamp_position():
    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    out = _to_internal_start_from(TimestampPosition(ts=ts))
    assert isinstance(out, _CommonTimestampPosition)
    assert out.ts == ts


def test_to_internal_start_from_passes_through_commitseq():
    p = CommitSeqPosition(seq="ab" * 16)
    assert _to_internal_start_from(p) is p


def test_to_internal_start_from_passes_through_table_commitseq():
    p = TableCommitSeqPosition(seqs={"CDSCHEMA.USERS_CD": "ab" * 16})
    assert _to_internal_start_from(p) is p


def test_to_internal_start_from_passes_through_relative_keywords():
    assert _to_internal_start_from("head") == "head"
    assert _to_internal_start_from("tail") == "tail"


def test_build_ingester_translates_timestamp_start_from():
    from tabsdatak.conn.common.sql.cdc.typing import (
        TimestampPosition as _CommonTimestampPosition,
    )

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    src = _src(start_from=TimestampPosition(ts=ts))

    with patch(
        "tabsdatak.conn.db2._cdc_plugin.SqlReplicaIngester",
    ) as mock_ingester_cls:
        Db2CdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    assert isinstance(kw["start_from"], _CommonTimestampPosition)
    assert kw["start_from"].ts == ts


def test_build_ingester_forwards_commitseq_position():
    src = _src(start_from=CommitSeqPosition(seq="cd" * 16))

    with patch(
        "tabsdatak.conn.db2._cdc_plugin.SqlReplicaIngester",
    ) as mock_ingester_cls:
        Db2CdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    assert isinstance(kw["start_from"], CommitSeqPosition)
    assert kw["start_from"].seq == "cd" * 16

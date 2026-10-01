#
# Copyright 2026 Tabsdata Inc.
#

"""``PostgresCdcStreamSrcPlugin`` -- assert the StreamSrcDef shape
and that ``run`` wires a Wal2JsonIngester + run_cdc with the right
config translated from the user-facing ``src_cfg``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc import CdcStageSrc
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import (
    LsnPosition,
    PostgresCdcStreamSrcConn,
    PostgresCdcStreamSrc,
)
from tabsdatak.conn.postgres._cdc_plugin import (
    POSTGRES_CDC_STREAM_SRC,
    PostgresCdcStreamSrcPlugin,
    _build_cdc_run_config,
    _to_internal_start_from,
)


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _conn() -> PostgresCdcStreamSrcConn:
    return PostgresCdcStreamSrcConn(
        uri="postgresql://h:5432/d",
        credentials=_creds(),
    )


def _src(**overrides) -> PostgresCdcStreamSrc:
    kw = dict(
        tables=["public.users", "public.orders"],
        replication_slot="tdcdc_slot",
        start_from="tail",
    )
    kw.update(overrides)
    return PostgresCdcStreamSrc(**kw)


# --- StreamSrcDef shape ---------------------------------------------


def test_stream_src_def_is_for_postgres_cdc():
    assert POSTGRES_CDC_STREAM_SRC.conn is PostgresCdcStreamSrcConn
    assert POSTGRES_CDC_STREAM_SRC.src is PostgresCdcStreamSrc
    assert POSTGRES_CDC_STREAM_SRC.plugin is PostgresCdcStreamSrcPlugin
    assert POSTGRES_CDC_STREAM_SRC.type_ == "postgres-cdc"
    assert POSTGRES_CDC_STREAM_SRC.publisher_src is CdcStageSrc


def test_stream_src_def_cardinality_matches_table_count():
    src = _src(tables=["s.t1", "s.t2", "s.t3"])
    assert POSTGRES_CDC_STREAM_SRC.cardinality(src) == 3


# --- _build_cdc_run_config from src_cfg -----------------------------


def test_run_config_uses_defaults_when_no_src_cfg():
    src = _src()
    cfg = _build_cdc_run_config(src)
    assert isinstance(cfg, CdcRunConfig)
    # Defaults come from tabsdatak.conn.common.sql.cdc.constants.
    assert cfg.buffer_max_rows == 10_000
    assert cfg.buffer_max_secs == 60
    assert cfg.poll_interval_seconds == 1
    assert cfg.max_large_text_length == 1024


def test_run_config_picks_up_src_cfg_overrides():
    src = _src(
        src_cfg={
            "tabsdata.cdc.buffer.max_rows": 5,
            "tabsdata.cdc.buffer.max_secs": 30,
            "tabsdata.cdc.poll_interval_seconds": 2,
            "tabsdata.cdc.max_large_text_length": 4096,
        },
    )
    cfg = _build_cdc_run_config(src)
    assert cfg.buffer_max_rows == 5
    assert cfg.buffer_max_secs == 30
    assert cfg.poll_interval_seconds == 2
    assert cfg.max_large_text_length == 4096


def test_run_config_carries_format_fields_from_src():
    from tabsdatak.conn.common.sql.cdc.typing import (
        CdcFormat as _CommonCdcFormat,
    )

    src = _src(columns_format="struct", flatten_columns=False)
    cfg = _build_cdc_run_config(src)
    # The runner sees a *common* CdcFormat composed by the plugin
    # from the two flat fields on PostgresCdcStreamSrc.
    assert isinstance(cfg.cdc_format, _CommonCdcFormat)
    assert cfg.cdc_format.columns_format == "struct"
    assert cfg.cdc_format.flatten_columns is False


def test_run_config_format_fields_default_to_columns_flattened():
    src = _src()
    cfg = _build_cdc_run_config(src)
    assert cfg.cdc_format.columns_format == "columns"
    assert cfg.cdc_format.flatten_columns is True


# --- logging_level wiring -------------------------------------------


def test_run_config_decodes_a_marked_trigger_max_secs():
    from tabsdatak.conn.common._stream._floor import unsafe_seconds

    src = _src(src_cfg={"tabsdata.cdc.trigger.max_secs": unsafe_seconds(2)})
    assert _build_cdc_run_config(src).trigger_max_secs == 2


@pytest.mark.parametrize(
    "key",
    [
        "tabsdata.cdc.buffer.max_rows",
        "tabsdata.cdc.buffer.max_bytes",
        "tabsdata.cdc.buffer.max_secs",
        "tabsdata.cdc.trigger.max_rows",
        "tabsdata.cdc.trigger.max_bytes",
    ],
)
def test_a_cdc_limit_that_is_not_positive_is_rejected(key):
    with pytest.raises(Exception, match="must be a positive whole number"):
        _src(src_cfg={key: 0})


def test_run_config_logging_level_default_is_error():
    import logging
    cfg = _build_cdc_run_config(_src())
    assert cfg.logging_level == logging.ERROR


def test_run_config_logging_level_picks_up_src_cfg_override():
    import logging
    src = _src(src_cfg={"postgres.cdc.logging_level": logging.DEBUG})
    cfg = _build_cdc_run_config(src)
    assert cfg.logging_level == logging.DEBUG


def test_plugin_run_applies_logging_level_to_connector_logger():
    import logging
    src = _src(src_cfg={"postgres.cdc.logging_level": logging.DEBUG})
    ctx = MagicMock()
    connector_logger = logging.getLogger("tabsdatak.conn.postgres._cdc_plugin")
    connector_logger.setLevel(logging.NOTSET)
    try:
        # ``run`` is inherited from ``CdcPlugin``; patch the shared loop so
        # ``run`` just does its setup (config + log levels + ingester) and
        # returns.
        with patch(
            "tabsdatak.conn.common._stream._plugin.PipelineStreamSrcPlugin.stream",
        ), patch(
            "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
        ):
            PostgresCdcStreamSrcPlugin().run(ctx=ctx, conn=_conn(), stream_src=src)
        assert connector_logger.level == logging.DEBUG
    finally:
        connector_logger.setLevel(logging.NOTSET)


# --- build_ingester wires the right Wal2JsonIngester ----------------


def test_build_ingester_constructs_with_slot_and_format_version():
    src = _src(
        replication_slot_behavior="create",
        publication_name="pub_users",
        src_cfg={
            "postgres.cdc.blocking_timeout_seconds": 7,
            "postgres.cdc.wal2json_format_version": 1,
            "postgres.cdc.unchanged_large_text_as_null": True,
        },
    )
    conn = _conn()

    with patch(
        "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
    ) as mock_ingester_cls:
        mock_ingester_cls.return_value = "ingester-instance"
        ingester = PostgresCdcStreamSrcPlugin().build_ingester(conn, src)

    mock_ingester_cls.assert_called_once()
    kw = mock_ingester_cls.call_args.kwargs
    assert kw["conn"] is conn
    assert kw["tables"] == ["public.users", "public.orders"]
    assert kw["start_from"] == "tail"
    assert kw["replication_slot"] == "tdcdc_slot"
    assert kw["replication_slot_behavior"] == "create"
    assert kw["publication_name"] == "pub_users"
    assert kw["blocking_timeout_seconds"] == 7
    assert kw["wal2json_format_version"] == 1
    assert kw["unchanged_large_text_as_null"] is True
    assert ingester == "ingester-instance"


def test_build_ingester_blocking_timeout_falls_back_to_default():
    src = _src()  # no src_cfg

    with patch(
        "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_BLOCKING_TIMEOUT_SECONDS from common-sql constants.
    assert kw["blocking_timeout_seconds"] == 1


def test_build_ingester_format_version_falls_back_to_default():
    src = _src()

    with patch(
        "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_WAL2JSON_FORMAT_VERSION = 2.
    assert kw["wal2json_format_version"] == 2


def test_build_ingester_unchanged_large_text_as_null_defaults_to_false():
    src = _src()

    with patch(
        "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().build_ingester(_conn(), src)

    assert mock_ingester_cls.call_args.kwargs["unchanged_large_text_as_null"] is False


# --- Public -> internal conversion at the plugin boundary -----------


def test_to_internal_start_from_passes_lsn_through():
    p = LsnPosition(lsn=42)
    assert _to_internal_start_from(p) is p


def test_to_internal_start_from_passes_through_relative_keywords():
    assert _to_internal_start_from("head") == "head"
    assert _to_internal_start_from("tail") == "tail"


def test_build_ingester_forwards_lsn_position_to_ingester():
    src = _src(start_from=LsnPosition(lsn=12345))

    with patch(
        "tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().build_ingester(_conn(), src)

    kw = mock_ingester_cls.call_args.kwargs
    assert isinstance(kw["start_from"], LsnPosition)
    assert kw["start_from"].lsn == 12345

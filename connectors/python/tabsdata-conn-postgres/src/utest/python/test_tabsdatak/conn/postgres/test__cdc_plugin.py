"""``PostgresCdcStreamSrcPlugin`` -- assert the StreamSrcDef shape
and that ``run`` wires a Wal2JsonIngester + run_cdc with the right
config translated from the user-facing ``src_cfg``."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import (
    LsnPosition,
    PostgresCdcConn,
    PostgresCdcStreamSrc,
)
from tabsdatak.conn.postgres._cdc_plugin import (
    POSTGRES_CDC_STREAM_SRC,
    PostgresCdcStreamSrcPlugin,
    _build_cdc_run_config,
    _to_internal_start_from,
)


def _creds() -> UserPassword:
    return UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))


def _src(**overrides) -> PostgresCdcStreamSrc:
    kw = dict(
        conn=PostgresCdcConn(
            uri="postgresql://h:5432/d",
            credentials=_creds(),
        ),
        tables=["public.users", "public.orders"],
        replication_slot="tdcdc_slot",
        start_from="tail",
    )
    kw.update(overrides)
    return PostgresCdcStreamSrc(**kw)


# --- StreamSrcDef shape ---------------------------------------------


def test_stream_src_def_is_for_postgres_cdc():
    assert POSTGRES_CDC_STREAM_SRC.conn is PostgresCdcConn
    assert POSTGRES_CDC_STREAM_SRC.src is PostgresCdcStreamSrc
    assert POSTGRES_CDC_STREAM_SRC.plugin is PostgresCdcStreamSrcPlugin
    assert POSTGRES_CDC_STREAM_SRC.src_type == "postgres-cdc"
    assert POSTGRES_CDC_STREAM_SRC.uses_manifest is True


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
    # from the two flat fields on PostgresCdcStreamSrc.
    assert isinstance(cfg.cdc_format, _CommonCdcFormat)
    assert cfg.cdc_format.columns_format == "struct"
    assert cfg.cdc_format.flatten_columns is False


def test_run_config_format_fields_default_to_columns_flattened():
    src = _src()
    cfg = _build_cdc_run_config(src)
    assert cfg.cdc_format.columns_format == "columns"
    assert cfg.cdc_format.flatten_columns is True


# --- Plugin.run wires the right Wal2JsonIngester --------------------


def test_plugin_run_constructs_ingester_with_slot_and_format_version():
    src = _src(
        replication_slot_behavior="create",
        publication_name="pub_users",
        src_cfg={
            "postgres.cdc.blocking_timeout_seconds": 7,
            "postgres.cdc.wal2json_format_version": 1,
        },
    )
    ctx = MagicMock()
    captured: dict = {}

    def fake_run(*, ctx, ingester, cfg):
        captured["ingester"] = ingester
        captured["cfg"] = cfg

    with patch(
        "tabsdatak.conn.postgres._cdc_plugin.run_cdc", side_effect=fake_run,
    ), patch(
        "tabsdatak.conn.postgres._cdc_plugin.Wal2JsonIngester",
    ) as mock_ingester_cls:
        mock_ingester_cls.return_value = "ingester-instance"
        PostgresCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)

    mock_ingester_cls.assert_called_once()
    kw = mock_ingester_cls.call_args.kwargs
    assert kw["conn"] is src.conn
    assert kw["tables"] == ["public.users", "public.orders"]
    assert kw["start_from"] == "tail"
    assert kw["replication_slot"] == "tdcdc_slot"
    assert kw["replication_slot_behavior"] == "create"
    assert kw["publication_name"] == "pub_users"
    assert kw["blocking_timeout_seconds"] == 7
    assert kw["wal2json_format_version"] == 1

    assert captured["ingester"] == "ingester-instance"
    assert isinstance(captured["cfg"], CdcRunConfig)


def test_plugin_run_blocking_timeout_falls_back_to_default():
    src = _src()  # no src_cfg
    ctx = MagicMock()

    with patch(
        "tabsdatak.conn.postgres._cdc_plugin.run_cdc",
    ), patch(
        "tabsdatak.conn.postgres._cdc_plugin.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_BLOCKING_TIMEOUT_SECONDS from common-sql constants.
    assert kw["blocking_timeout_seconds"] == 1


def test_plugin_run_format_version_falls_back_to_default():
    src = _src()
    ctx = MagicMock()

    with patch(
        "tabsdatak.conn.postgres._cdc_plugin.run_cdc",
    ), patch(
        "tabsdatak.conn.postgres._cdc_plugin.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)

    kw = mock_ingester_cls.call_args.kwargs
    # DEFAULT_WAL2JSON_FORMAT_VERSION = 2.
    assert kw["wal2json_format_version"] == 2


# --- Public -> internal conversion at the plugin boundary -----------


def test_to_internal_start_from_passes_lsn_through():
    p = LsnPosition(lsn=42)
    assert _to_internal_start_from(p) is p


def test_to_internal_start_from_passes_through_relative_keywords():
    assert _to_internal_start_from("head") == "head"
    assert _to_internal_start_from("tail") == "tail"


def test_plugin_run_forwards_lsn_position_to_ingester():
    src = _src(start_from=LsnPosition(lsn=12345))
    ctx = MagicMock()

    with patch(
        "tabsdatak.conn.postgres._cdc_plugin.run_cdc",
    ), patch(
        "tabsdatak.conn.postgres._cdc_plugin.Wal2JsonIngester",
    ) as mock_ingester_cls:
        PostgresCdcStreamSrcPlugin().run(ctx=ctx, stream_src=src)

    kw = mock_ingester_cls.call_args.kwargs
    assert isinstance(kw["start_from"], LsnPosition)
    assert kw["start_from"].lsn == 12345

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging

import polars as pl
import pytest
from test_tabsdatak.conn.awsglue.support import FakeCtx, conn

from tabsdatak.conn.awsglue import AwsGlueDest, AwsGlueDestConn

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _impl as impl

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _plugin as plugin
from tabsdatak.conn.awsglue._explorer import AwsGlueConnectionExplorer
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode, AwsGlueRuntimeException

pytestmark = pytest.mark.conn_awsglue


def _frame() -> pl.LazyFrame:
    return pl.LazyFrame({"id": [1]})


def _captured(monkeypatch) -> dict:
    captured: dict = {}

    def write_all_tables(cfg, frames):
        captured["cfg"] = cfg
        captured["frames"] = frames

    monkeypatch.setattr(impl, "write_all_tables", write_all_tables)
    return captured


def test_def_binds_the_destination_to_its_connection_and_plugin():
    definition = plugin.AWSGLUE_DEST
    assert definition.type_ == "awsglue-iceberg-out"
    assert definition.system == "AWS Glue"
    assert definition.conn is AwsGlueDestConn
    assert definition.dest is AwsGlueDest
    assert definition.plugin is plugin.AwsGlueDestPlugin
    assert definition.explorer is AwsGlueConnectionExplorer


def test_def_counts_one_slot_per_table():
    dest = AwsGlueDest(tables=["orders", "customers"])
    assert plugin.AWSGLUE_DEST.cardinality(dest) == 2


def test_write_out_hands_the_writer_what_the_destination_resolves(monkeypatch):
    captured = _captured(monkeypatch)
    connection = conn()
    dest = AwsGlueDest(
        tables=["orders", "returns.orders"],
        if_table_exists="replace",
        schema_evolution="update",
    )
    frames = [_frame(), _frame()]
    plugin.AwsGlueDestPlugin().write_out(FakeCtx("trx-9"), connection, dest, frames)
    cfg = captured["cfg"]
    assert cfg.conn is connection
    assert cfg.tables == ["orders", "returns.orders"]
    assert cfg.if_table_exists == "replace"
    assert cfg.schema_evolution == "update"
    assert cfg.transaction_id == "trx-9"
    assert captured["frames"] is frames


def test_write_out_defaults_to_a_strict_append(monkeypatch):
    captured = _captured(monkeypatch)
    dest = AwsGlueDest(tables=["orders"])
    plugin.AwsGlueDestPlugin().write_out(FakeCtx(), conn(), dest, [_frame()])
    assert captured["cfg"].if_table_exists == "append"
    assert captured["cfg"].schema_evolution == "strict"


def test_write_out_refuses_a_slot_count_that_differs_from_the_tables(monkeypatch):
    _captured(monkeypatch)
    dest = AwsGlueDest(tables=["orders", "customers"])
    with pytest.raises(AwsGlueRuntimeException) as raised:
        plugin.AwsGlueDestPlugin().write_out(FakeCtx(), conn(), dest, [_frame()])
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_6


def test_write_out_lets_a_connector_error_through(monkeypatch):
    def write_all_tables(cfg, frames):
        raise AwsGlueErrorCode.AWSGLUE_8.exception(table="orders")

    monkeypatch.setattr(impl, "write_all_tables", write_all_tables)
    dest = AwsGlueDest(tables=["orders"])
    with pytest.raises(AwsGlueRuntimeException) as raised:
        plugin.AwsGlueDestPlugin().write_out(FakeCtx(), conn(), dest, [_frame()])
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_8


def test_write_out_wraps_any_other_failure(monkeypatch):
    failure = RuntimeError("boom")

    def write_all_tables(cfg, frames):
        raise failure

    monkeypatch.setattr(impl, "write_all_tables", write_all_tables)
    dest = AwsGlueDest(tables=["orders", "customers"])
    with pytest.raises(AwsGlueRuntimeException) as raised:
        plugin.AwsGlueDestPlugin().write_out(
            FakeCtx(), conn(), dest, [_frame(), _frame()]
        )
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_15
    assert raised.value.__cause__ is failure
    assert "orders, customers" in str(raised.value)


def test_logging_level_defaults_to_error():
    assert plugin._logging_level(None) == logging.ERROR


def test_logging_level_is_read_from_the_dest_cfg():
    assert plugin._logging_level({"awsglue.logging_level": logging.DEBUG}) == (
        logging.DEBUG
    )

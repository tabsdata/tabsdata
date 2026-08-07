#
# Copyright 2026 Tabsdata Inc.
#

"""``_stream_impl.py`` -- consumer + buffer + kafka-config builder
pure-function coverage. Real broker calls live in
``test__stream_plugin_integration.py``."""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    MessagesBuffer,
    RolloverReason,
    build_kafka_configs,
)
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC


pytestmark = pytest.mark.kafka


def _creds():
    return UserPassword(user=Secret("u"), password=Secret("p"))


# --- MessagesBuffer -------------------------------------------------


def _allow_subset(a, b):
    return set(b).issubset(set(a))


def test_messages_buffer_first_append_sets_schema_id():
    b = MessagesBuffer("evt", _allow_subset)
    assert b.append((10, {"a": 1}, ("a",))) is True
    assert b.message_count == 1


def test_messages_buffer_compatible_append_grows_buffer():
    b = MessagesBuffer("evt", _allow_subset)
    b.append((10, {"a": 1, "b": 2}, ("a", "b")))
    assert b.append((10, {"a": 3}, ("a",))) is True
    assert b.message_count == 2


def test_messages_buffer_incompatible_append_returns_false():
    b = MessagesBuffer("evt", _allow_subset)
    b.append((10, {"a": 1}, ("a",)))
    assert b.append((10, {"a": 1, "z": 9}, ("a", "z"))) is False
    assert b.message_count == 1


def test_messages_buffer_size_mb_aggregates_payload_bytes():
    b = MessagesBuffer("evt", _allow_subset)
    b.append((2 * 1024 * 1024, {"a": 1}, ("a",)))
    assert b.size_mb == 2


def test_messages_buffer_to_parquet_returns_file(tmp_path):
    b = MessagesBuffer("evt", _allow_subset)
    b.append((10, {"a": 1, "b": "x"}, ("a", "b")))
    b.append((10, {"a": 2, "b": "y"}, ("a", "b")))
    path = b.to_parquet(dir_path=tmp_path)
    assert path is not None
    assert path.exists()
    assert path.suffix == ".parquet"
    assert b.is_flushed is True


def test_messages_buffer_to_parquet_empty_returns_none(tmp_path):
    b = MessagesBuffer("evt", _allow_subset)
    assert b.to_parquet(dir_path=tmp_path) is None


def test_messages_buffer_to_parquet_leads_with_the_topic_column(tmp_path):
    import polars as pl

    b = MessagesBuffer("evt", _allow_subset)
    b.append((10, {"a": 1, "b": "x"}, ("a", "b")))
    b.append((10, {"a": 2, "b": "y"}, ("a", "b")))
    frame = pl.read_parquet(b.to_parquet(dir_path=tmp_path))

    assert frame.columns == [KAFKA_META_TOPIC, "a", "b"]
    assert frame[KAFKA_META_TOPIC].to_list() == ["evt", "evt"]
    # The payload columns are untouched.
    assert frame["a"].to_list() == [1, 2]
    assert frame["b"].to_list() == ["x", "y"]


# --- build_kafka_configs --------------------------------------------


def test_build_kafka_configs_string_servers():
    conn = KafkaStreamSrcConn(servers="a:9092", credentials=_creds(), group_id="g1")
    cfg = build_kafka_configs(conn=conn)
    assert cfg["bootstrap.servers"] == "a:9092"
    assert cfg["group.id"] == "g1"


def test_build_kafka_configs_comma_separated_servers():
    conn = KafkaStreamSrcConn(
        servers="a:9092,b:9092", credentials=_creds(), group_id="g1",
    )
    cfg = build_kafka_configs(conn=conn)
    assert cfg["bootstrap.servers"] == "a:9092,b:9092"


def test_build_kafka_configs_no_credentials_skips_sasl_keys():
    conn = KafkaStreamSrcConn(servers="a:9092", group_id="g1")
    cfg = build_kafka_configs(conn=conn)
    assert "sasl.username" not in cfg
    assert "sasl.password" not in cfg
    assert "security.protocol" not in cfg


def test_build_kafka_configs_with_credentials_sets_sasl_keys():
    conn = KafkaStreamSrcConn(servers="a:9092", credentials=_creds(), group_id="g1")
    cfg = build_kafka_configs(conn=conn)
    assert cfg["security.protocol"] == "sasl_plaintext"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-256"
    assert cfg["sasl.username"] == "u"
    assert cfg["sasl.password"] == "p"


def test_build_kafka_configs_resolves_secret_servers_and_group_id():
    conn = KafkaStreamSrcConn(
        servers=Secret("a:9092"),
        group_id=Secret("g1"),
    )
    cfg = build_kafka_configs(conn=conn)
    assert cfg["bootstrap.servers"] == "a:9092"
    assert cfg["group.id"] == "g1"


def test_build_kafka_configs_force_auto_commit_off():
    conn = KafkaStreamSrcConn(servers="a:9092", group_id="g1")
    cfg = build_kafka_configs(conn=conn)
    assert cfg["enable.auto.commit"] is False
    assert cfg["auto.offset.reset"] == "earliest"


def test_build_kafka_configs_conn_cfg_overrides_pass_through():
    conn = KafkaStreamSrcConn(
        servers="a:9092",
        group_id="g1",
        conn_cfg={"session.timeout.ms": 30000},
    )
    cfg = build_kafka_configs(conn=conn)
    assert cfg["session.timeout.ms"] == 30000


# --- RolloverReason enum --------------------------------------------


def test_rollover_reason_values():
    assert RolloverReason.SchemaChange.value == "SchemaChange"
    assert RolloverReason.Size.value == "Size"
    assert RolloverReason.Messages.value == "Messages"
    assert RolloverReason.Time.value == "Time"

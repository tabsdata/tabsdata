"""``_stream_impl.py`` -- consumer + buffer + kafka-config builder
pure-function coverage. Real broker calls live in
``test__stream_plugin_integration.py``."""

from __future__ import annotations

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    MessagesBuffer,
    RolloverReason,
    build_kafka_configs,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


def _creds():
    return UserPassword(user=Secret.lit("u"), password=Secret.lit("p"))


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


# --- build_kafka_configs --------------------------------------------


def test_build_kafka_configs_string_servers():
    conn = KafkaConn(servers="a:9092", credentials=_creds(), group_id="g1")
    cfg = build_kafka_configs(conn=conn, group_id="ignored")
    assert cfg["bootstrap.servers"] == "a:9092"
    assert cfg["group.id"] == "g1"  # conn wins (enforce=True default)


def test_build_kafka_configs_list_servers_joined_with_commas():
    conn = KafkaConn(
        servers=["a:9092", "b:9092"], credentials=_creds(),
        group_id="g1",
    )
    cfg = build_kafka_configs(conn=conn, group_id="ignored")
    assert cfg["bootstrap.servers"] == "a:9092,b:9092"


def test_build_kafka_configs_no_credentials_skips_sasl_keys():
    conn = KafkaConn(servers="a:9092", group_id="g1")
    cfg = build_kafka_configs(conn=conn, group_id="ignored")
    assert "sasl.username" not in cfg
    assert "sasl.password" not in cfg
    assert "security.protocol" not in cfg


def test_build_kafka_configs_with_credentials_sets_sasl_keys():
    conn = KafkaConn(servers="a:9092", credentials=_creds(), group_id="g1")
    cfg = build_kafka_configs(conn=conn, group_id="ignored")
    assert cfg["security.protocol"] == "sasl_plaintext"
    assert cfg["sasl.mechanism"] == "SCRAM-SHA-256"
    assert cfg["sasl.username"] == "u"
    assert cfg["sasl.password"] == "p"


def test_build_kafka_configs_enforce_true_conn_group_wins():
    conn = KafkaConn(
        servers="a:9092", group_id="conn-group",
        enforce_connection_params=True,
    )
    cfg = build_kafka_configs(conn=conn, group_id="src-group")
    assert cfg["group.id"] == "conn-group"


def test_build_kafka_configs_enforce_false_src_group_wins():
    conn = KafkaConn(
        servers="a:9092", group_id="conn-group",
        enforce_connection_params=False,
    )
    cfg = build_kafka_configs(conn=conn, group_id="src-group")
    assert cfg["group.id"] == "src-group"


def test_build_kafka_configs_missing_group_id_raises_kafka_8():
    conn = KafkaConn(servers="a:9092", enforce_connection_params=False)
    with pytest.raises(TabsdataException) as ei:
        build_kafka_configs(conn=conn, group_id=None)
    assert ei.value.api_error is KafkaErrorCode.KAFKA_8


def test_build_kafka_configs_force_auto_commit_off():
    conn = KafkaConn(servers="a:9092", group_id="g1")
    cfg = build_kafka_configs(conn=conn, group_id="g1")
    assert cfg["enable.auto.commit"] is False
    assert cfg["auto.offset.reset"] == "earliest"


def test_build_kafka_configs_conn_cfg_overrides_pass_through():
    conn = KafkaConn(
        servers="a:9092",
        group_id="g1",
        conn_cfg={"session.timeout.ms": 30000},
    )
    cfg = build_kafka_configs(conn=conn, group_id="g1")
    assert cfg["session.timeout.ms"] == 30000


# --- RolloverReason enum --------------------------------------------


def test_rollover_reason_values():
    assert RolloverReason.SchemaChange.value == "SchemaChange"
    assert RolloverReason.Size.value == "Size"
    assert RolloverReason.Messages.value == "Messages"
    assert RolloverReason.Time.value == "Time"

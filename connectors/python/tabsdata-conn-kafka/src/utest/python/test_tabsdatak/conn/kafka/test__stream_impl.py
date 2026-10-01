#
# Copyright 2026 Tabsdata Inc.
#

"""``_stream_impl.py`` -- encoder, position, ingester (consumer
patched out) and kafka-config builder coverage. Real broker calls
live in the integration tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    KafkaEncoder,
    KafkaIngester,
    KafkaPosition,
    KafkaRecord,
    build_kafka_configs,
)
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.kafka


def _creds():
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _allow_subset(a, b):
    return set(b).issubset(set(a))


def _message(
    value: dict | None = None,
    *,
    offset: int = 0,
    partition: int = 0,
    error=None,
    raw=None,
):
    message = MagicMock()
    message.error.return_value = error
    message.value.return_value = (
        raw if raw is not None else json.dumps(value).encode("utf-8")
    )
    message.topic.return_value = "evt"
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.key.return_value = None
    message.timestamp.return_value = (0, 0)
    return message


def _record(value: dict, *, offset: int = 0) -> KafkaRecord:
    return KafkaRecord(
        value=value,
        schema_id=tuple(sorted(value)),
        size=10,
        message=_message(value, offset=offset),
    )


# --- KafkaEncoder ---------------------------------------------------


def _frames(encoder: KafkaEncoder, records: list[KafkaRecord]) -> list[pl.DataFrame]:
    return [pl.from_arrow(table) for table in encoder.encode("evt", records)]


def test_every_record_goes_to_the_topic_slot():
    encoder = KafkaEncoder("evt", _allow_subset)
    assert encoder.slot(_record({"a": 1})) == "evt"


def test_a_record_size_is_its_payload_size():
    assert KafkaEncoder("evt", _allow_subset).size(_record({"a": 1})) == 10


def test_a_record_offset_is_the_next_offset_of_its_partition():
    record = _record({"a": 1}, offset=4)
    offset = KafkaEncoder("evt", _allow_subset).offset(record)
    assert offset.offsets == {"evt": {0: 5}}


def test_compatible_records_encode_into_one_table_led_by_the_topic():
    encoder = KafkaEncoder("evt", _allow_subset)
    (frame,) = _frames(encoder, [_record({"b": "x", "a": 1}), _record({"a": 2})])
    assert frame.columns == [KAFKA_META_TOPIC, "a", "b"]
    assert frame[KAFKA_META_TOPIC].to_list() == ["evt", "evt"]
    assert frame["a"].to_list() == [1, 2]
    assert frame["b"].to_list() == ["x", None]


def test_each_run_of_compatible_records_encodes_into_its_own_table():
    encoder = KafkaEncoder("evt", _allow_subset)
    frames = _frames(
        encoder,
        [
            _record({"a": 1}),
            _record({"a": 2, "z": 9}),
            _record({"a": 3, "z": 8}),
            _record({"a": 4, "y": 7}),
        ],
    )
    assert [frame.columns for frame in frames] == [
        [KAFKA_META_TOPIC, "a"],
        [KAFKA_META_TOPIC, "a", "z"],
        [KAFKA_META_TOPIC, "a", "y"],
    ]
    assert [frame["a"].to_list() for frame in frames] == [[1], [2, 3], [4]]


def test_compatibility_is_checked_against_the_first_record_of_a_run():
    encoder = KafkaEncoder("evt", _allow_subset)
    frames = _frames(
        encoder,
        [_record({"a": 1, "b": 1}), _record({"a": 2}), _record({"b": 3})],
    )
    assert [frame.height for frame in frames] == [3]


# --- KafkaPosition --------------------------------------------------


def test_a_position_keeps_the_highest_offset_of_every_partition():
    position = KafkaPosition({"evt": {0: 5, 1: 9}})
    position.update(KafkaPosition({"evt": {0: 7, 1: 3, 2: 1}, "other": {0: 2}}))
    assert position.offsets == {"evt": {0: 7, 1: 9, 2: 1}, "other": {0: 2}}


def test_a_position_after_a_message_points_past_it():
    position = KafkaPosition.after(_message({"a": 1}, offset=7, partition=2))
    assert position.offsets == {"evt": {2: 8}}


def test_a_position_is_written_as_a_topic_partition_map():
    position = KafkaPosition({"evt": {3: 17, 0: 42}})
    assert json.loads(position.write()) == {"evt": {"0": 42, "3": 17}}
    assert KafkaPosition().write() == "{}"


def test_a_written_position_reads_back_equal():
    position = KafkaPosition({"evt": {0: 42, 3: 17}, "other": {1: 0}})
    assert KafkaPosition.read(position.write()).offsets == position.offsets


@pytest.mark.parametrize(
    "state",
    [
        "not json",
        "[]",
        '{"evt": 3}',
        '{"evt": {"zero": 1}}',
        '{"evt": {"-1": 1}}',
        '{"evt": {"0": -1}}',
        '{"evt": {"0": 1.5}}',
        '{"evt": {"0": true}}',
    ],
)
def test_a_malformed_state_raises_kafka_25(state):
    with pytest.raises(TabsdataException) as raised:
        KafkaPosition.read(state)
    assert raised.value.api_error is KafkaErrorCode.KAFKA_25


# --- KafkaIngester --------------------------------------------------


def _conn() -> KafkaStreamSrcConn:
    return KafkaStreamSrcConn(servers="broker:9092", group_id="g1")


def _ingester(*, dlq_topic=None, consume_timeout_secs=2) -> KafkaIngester:
    return KafkaIngester(
        conn=_conn(),
        stream_src=KafkaStreamSrc(
            topic="evt", data_format="json", schema="", dlq_topic=dlq_topic
        ),
        consume_timeout_secs=consume_timeout_secs,
        connection_timeout_secs=16,
    )


def _opened(consumer, tmp_path, **kwargs) -> KafkaIngester:
    ingester = _ingester(**kwargs)
    with patch("confluent_kafka.Consumer", return_value=consumer):
        assert ingester.open_session(tmp_path) is consumer
    return ingester


def test_opening_a_session_subscribes_to_the_topic(tmp_path):
    consumer = MagicMock()
    _opened(consumer, tmp_path)
    consumer.list_topics.assert_called_once_with(timeout=16)
    consumer.subscribe.assert_called_once_with(["evt"])


def test_an_unreachable_broker_raises_kafka_6(tmp_path):
    consumer = MagicMock()
    consumer.list_topics.side_effect = RuntimeError("broker down")
    with pytest.raises(TabsdataException) as raised:
        _opened(consumer, tmp_path)
    assert raised.value.api_error is KafkaErrorCode.KAFKA_6


def test_polling_yields_the_decoded_messages_until_none_arrives(tmp_path):
    consumer = MagicMock()
    first, second = _message({"a": 1}, offset=1), _message({"a": 2}, offset=2)
    consumer.poll.side_effect = [first, second, None]
    ingester = _opened(consumer, tmp_path)

    records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [{"a": 1}, {"a": 2}]
    assert [record.message for record in records] == [first, second]
    assert records[0].size == len(first.value())


def test_polling_stops_once_the_consume_timeout_elapses(tmp_path):
    consumer = MagicMock()
    consumer.poll.return_value = _message({"a": 1})
    ingester = _opened(consumer, tmp_path, consume_timeout_secs=0)

    assert list(ingester.poll_changes(consumer)) == []
    consumer.poll.assert_not_called()


def test_broker_errors_and_undecodable_messages_go_to_the_dlq(tmp_path):
    consumer = MagicMock()
    broken = _message(error="boom")
    garbled = _message(raw=b"not json")
    good = _message({"a": 1})
    consumer.poll.side_effect = [broken, garbled, good, None]
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")

    records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [{"a": 1}]
    assert producer.produce.call_count == 2
    assert {call.kwargs["topic"] for call in producer.produce.call_args_list} == {
        "evt_dlq"
    }


def test_the_broker_keeps_the_offset():
    ingester = _ingester()
    assert ingester.resume_offset(first_stage=False, stream_state={"x": 1}) is None
    assert ingester.base_offset().offsets == {}


def _committed(consumer) -> list[tuple[str, int, int]]:
    (call,) = consumer.commit.call_args_list
    assert call.kwargs["asynchronous"] is False
    return [(tp.topic, tp.partition, tp.offset) for tp in call.kwargs["offsets"]]


def test_the_commit_state_commits_every_partition_staged(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    position = KafkaPosition({"evt": {0: 5, 2: 3}})

    ingester.commit_state(position)()

    assert _committed(consumer) == [("evt", 0, 5), ("evt", 2, 3)]


def test_the_commit_state_commits_what_was_staged_when_it_was_taken(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    position = KafkaPosition({"evt": {0: 2}})

    commit = ingester.commit_state(position)
    position.update(KafkaPosition({"evt": {0: 3, 1: 1}}))
    commit()

    assert _committed(consumer) == [("evt", 0, 2)]


def test_a_commit_state_without_offsets_commits_nothing(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    ingester.commit_state(KafkaPosition())()
    consumer.commit.assert_not_called()


def test_closing_the_session_closes_the_consumer_and_flushes_the_dlq(tmp_path):
    consumer = MagicMock()
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")
    ingester.close_session(consumer)
    consumer.close.assert_called_once()
    producer.flush.assert_called()


# --- build_kafka_configs --------------------------------------------


def test_build_kafka_configs_string_servers():
    conn = KafkaStreamSrcConn(servers="a:9092", credentials=_creds(), group_id="g1")
    cfg = build_kafka_configs(conn=conn)
    assert cfg["bootstrap.servers"] == "a:9092"
    assert cfg["group.id"] == "g1"


def test_build_kafka_configs_comma_separated_servers():
    conn = KafkaStreamSrcConn(
        servers="a:9092,b:9092",
        credentials=_creds(),
        group_id="g1",
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

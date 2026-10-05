#
# Copyright 2026 Tabsdata Inc.
#

"""``_stream_impl.py`` -- encoder, position, ingester (consumer
patched out) and kafka-config builder coverage. Real broker calls
live in the integration tests."""

from __future__ import annotations

import json
import sys
import time
from unittest.mock import MagicMock, patch

import polars as pl
import pyarrow as pa
import pytest

from tabsdatak.api import Secret
# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._lifecycle import StreamLifecycle

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._restart import StreamRestart
from tabsdatak.conn.common.error import ConnErrorCode
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._stream_impl import (
    KafkaEncoder,
    KafkaIngester,
    KafkaOffset,
    KafkaRecord,
    build_kafka_configs,
    retriable_acknowledgement,
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


def _record(
    value: dict,
    *,
    offset: int = 0,
    partition: int = 0,
    topic: str = "evt",
    schema_id=None,
) -> KafkaRecord:
    return KafkaRecord(
        value=value,
        schema_id=schema_id if schema_id is not None else tuple(sorted(value)),
        topic=topic,
        partition=partition,
        offset=offset,
    )


def _same_schema(a, b):
    return a == b


def _refuse(record, error):
    raise AssertionError(f"unexpected rejection of {record}: {error}")


def _encoder(compatible=_allow_subset, *, topic="evt", reject=_refuse) -> KafkaEncoder:
    return KafkaEncoder(topic, compatible, reject)


# --- KafkaEncoder ---------------------------------------------------


def _frames(encoder: KafkaEncoder, records: list[KafkaRecord]) -> list[pl.DataFrame]:
    return [pl.from_arrow(table) for table in encoder.encode("evt", records)]


def test_every_record_goes_to_the_topic_slot():
    encoder = _encoder()
    assert encoder.slot(_record({"a": 1})) == "evt"


def test_a_record_size_is_the_memory_its_value_holds():
    encoder = _encoder()
    small, large = _record({"a": "x"}), _record({"a": "x" * 1000})
    assert encoder.size(small) == sys.getsizeof(small)
    assert encoder.size(large) - encoder.size(small) == (
        sys.getsizeof("x" * 1000) - sys.getsizeof("x")
    )
    assert encoder.size(_record({"a": 1})) > encoder.size(
        KafkaRecord(value=None, schema_id=None, topic="evt", partition=0, offset=0)
    )


def test_a_record_offset_is_the_next_offset_of_its_partition():
    record = _record({"a": 1}, offset=4)
    offset = _encoder().offset(record)
    assert offset.offsets == {"evt": {0: 5}}


def test_compatible_records_encode_into_one_table_led_by_the_topic():
    encoder = _encoder()
    (frame,) = _frames(encoder, [_record({"b": "x", "a": 1}), _record({"a": 2})])
    assert frame.columns == [KAFKA_META_TOPIC, "a", "b"]
    assert frame[KAFKA_META_TOPIC].to_list() == ["evt", "evt"]
    assert frame["a"].to_list() == [1, 2]
    assert frame["b"].to_list() == ["x", None]


def test_each_run_of_compatible_records_encodes_into_its_own_table():
    encoder = _encoder()
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


def test_the_columns_are_the_union_of_the_keys_of_the_group():
    encoder = _encoder(_same_schema)
    (frame,) = _frames(
        encoder,
        [
            _record({"id": 1}, schema_id=7),
            _record({"id": 2, "name": "bob"}, schema_id=7),
            _record({"id": 3, "tags": ["x"]}, schema_id=7),
        ],
    )
    assert frame.columns == [KAFKA_META_TOPIC, "id", "name", "tags"]
    assert frame["name"].to_list() == [None, "bob", None]
    assert frame["tags"].to_list() == [None, None, ["x"]]


def test_the_topic_column_holds_the_topic_of_each_message():
    encoder = _encoder(topic="^evt.*")
    (frame,) = _frames(encoder, [_record({"a": 1}, topic="evt.a")])
    assert frame[KAFKA_META_TOPIC].to_list() == ["evt.a"]


def test_records_are_grouped_by_topic_before_schema():
    encoder = _encoder(topic="^evt.*")
    frames = _frames(
        encoder,
        [
            _record({"a": 1}, topic="evt.a"),
            _record({"b": 1}, topic="evt.b"),
            _record({"a": 2}, topic="evt.a"),
            _record({"b": 2, "c": 1}, topic="evt.b"),
        ],
    )
    assert [frame[KAFKA_META_TOPIC].to_list() for frame in frames] == [
        ["evt.a", "evt.a"],
        ["evt.b"],
        ["evt.b"],
    ]
    assert frames[0]["a"].to_list() == [1, 2]
    assert [frame.columns for frame in frames[1:]] == [
        [KAFKA_META_TOPIC, "b"],
        [KAFKA_META_TOPIC, "b", "c"],
    ]


def test_compatibility_is_checked_against_the_first_record_of_a_run():
    encoder = _encoder()
    frames = _frames(
        encoder,
        [_record({"a": 1, "b": 1}), _record({"a": 2}), _record({"b": 3})],
    )
    assert [frame.height for frame in frames] == [3]


# --- KafkaEncoder: schema stability and rejections -----------------


def _encoded(encoder: KafkaEncoder, *groups: list[KafkaRecord]) -> list[pl.DataFrame]:
    frames = []
    for group in groups:
        frames.extend(_frames(encoder, group))
    return frames


def test_a_slot_keeps_one_schema_across_groups_of_a_schema_id():
    encoder = _encoder(_same_schema)
    first, second = _encoded(
        encoder,
        [_record({"a": 1, "b": None}, schema_id=7)],
        [_record({"a": 2.5, "b": "x"}, schema_id=7)],
    )
    assert first.schema["b"] == pl.Null
    assert second["a"].dtype == pl.Float64
    assert second["b"].dtype == pl.String
    assert encoder.schemas[("evt", 7)].field("a").type == pa.float64()


def test_a_column_missing_from_a_later_group_stays_typed_and_null():
    encoder = _encoder(_same_schema)
    _, second = _encoded(
        encoder,
        [_record({"a": 1, "b": "x"}, schema_id=7)],
        [_record({"a": 2}, schema_id=7)],
    )
    assert second.columns == [KAFKA_META_TOPIC, "a", "b"]
    assert second["b"].dtype == pl.String
    assert second["b"].to_list() == [None]


def test_a_new_schema_id_starts_its_own_schema():
    encoder = _encoder(_same_schema)
    _, second = _encoded(
        encoder,
        [_record({"a": 1}, schema_id=7)],
        [_record({"a": "x"}, schema_id=8)],
    )
    assert second["a"].dtype == pl.String


def test_a_row_that_does_not_fit_is_rejected_and_the_rest_written():
    rejected = []
    encoder = _encoder(_same_schema, reject=lambda r, e: rejected.append((r, e)))
    (frame,) = _frames(
        encoder,
        [
            _record({"a": 1}, offset=1, schema_id=7),
            _record({"a": "x"}, offset=2, schema_id=7),
            _record({"a": 3}, offset=3, schema_id=7),
        ],
    )
    assert frame["a"].to_list() == [1, 3]
    assert [record.offset for record, _ in rejected] == [2]
    assert isinstance(rejected[0][1], (pa.ArrowInvalid, pa.ArrowTypeError))


def test_a_row_conflicting_with_the_known_schema_is_rejected():
    rejected = []
    encoder = _encoder(_same_schema, reject=lambda r, e: rejected.append(r))
    first, second = _encoded(
        encoder,
        [_record({"a": 1}, schema_id=7)],
        [_record({"a": {"k": 1}}, offset=9, schema_id=7), _record({"a": 2}, schema_id=7)],
    )
    assert second["a"].to_list() == [2]
    assert [record.offset for record in rejected] == [9]


def test_a_group_rejected_entirely_writes_nothing():
    encoder = _encoder(_same_schema, reject=lambda r, e: None)
    first = _frames(encoder, [_record({"a": 1}, schema_id=7)])
    nothing = _frames(encoder, [_record({"a": "x"}, schema_id=7)])
    assert len(first) == 1 and nothing == []


# --- KafkaOffset --------------------------------------------------


def test_a_position_keeps_the_highest_offset_of_every_partition():
    position = KafkaOffset({"evt": {0: 5, 1: 9}})
    position.update(KafkaOffset({"evt": {0: 7, 1: 3, 2: 1}, "other": {0: 2}}))
    assert position.offsets == {"evt": {0: 7, 1: 9, 2: 1}, "other": {0: 2}}


def test_a_position_after_a_message_points_past_it():
    position = KafkaOffset.after(_record({"a": 1}, offset=7, partition=2))
    assert position.offsets == {"evt": {2: 8}}


def test_a_position_is_written_as_a_topic_partition_map():
    position = KafkaOffset({"evt": {3: 17, 0: 42}})
    assert json.loads(position.write()) == {"evt": {"0": 42, "3": 17}}
    assert KafkaOffset().write() == "{}"


def test_a_written_position_reads_back_equal():
    position = KafkaOffset({"evt": {0: 42, 3: 17}, "other": {1: 0}})
    assert KafkaOffset.read(position.write()).offsets == position.offsets


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
        KafkaOffset.read(state)
    assert raised.value.api_error is KafkaErrorCode.KAFKA_25


# --- KafkaIngester --------------------------------------------------


def _conn(conn_cfg=None) -> KafkaStreamSrcConn:
    return KafkaStreamSrcConn(servers="broker:9092", group_id="g1", conn_cfg=conn_cfg)


def _ingester(*, dlq_topic=None, poll_max_secs=2, conn_cfg=None) -> KafkaIngester:
    return KafkaIngester(
        conn=_conn(conn_cfg),
        stream_src=KafkaStreamSrc(
            topic="evt", data_format="json", schema="", dlq_topic=dlq_topic
        ),
        poll_max_secs=poll_max_secs,
        connect_timeout_secs=16,
        consume_max_messages=1000,
    )


def _opened(consumer, tmp_path, **kwargs) -> KafkaIngester:
    from confluent_kafka import TopicPartition

    consumer.assignment.return_value = [TopicPartition("evt", p) for p in range(4)]
    ingester = _ingester(**kwargs)
    with patch("confluent_kafka.Consumer", return_value=consumer):
        assert ingester.open_session(tmp_path) is consumer
    return ingester


class Settling(StreamLifecycle):
    def __init__(self) -> None:
        self.settled = 0

    def settle(self) -> None:
        self.settled += 1


def _retriable(reason: str):
    from confluent_kafka import KafkaError, KafkaException

    return KafkaException(KafkaError(KafkaError._TRANSPORT, reason, False, True))


def _final(reason: str):
    from confluent_kafka import KafkaError, KafkaException

    return KafkaException(
        KafkaError(KafkaError.UNKNOWN_MEMBER_ID, reason, False, False)
    )


def test_a_retriable_kafka_error_is_a_retriable_acknowledgement():
    assert retriable_acknowledgement(_retriable("broker away"))
    assert not retriable_acknowledgement(_final("not a member"))
    assert not retriable_acknowledgement(RuntimeError("boom"))


def test_opening_a_session_subscribes_to_the_topic(tmp_path):
    consumer = MagicMock()
    _opened(consumer, tmp_path)
    consumer.list_topics.assert_called_once_with(timeout=16)
    assert consumer.subscribe.call_args.args == (["evt"],)


def test_an_unreachable_broker_raises_kafka_6(tmp_path):
    consumer = MagicMock()
    consumer.list_topics.side_effect = RuntimeError("broker down")
    with pytest.raises(TabsdataException) as raised:
        _opened(consumer, tmp_path)
    assert raised.value.api_error is KafkaErrorCode.KAFKA_6


def test_polling_yields_the_decoded_messages_until_none_arrives(tmp_path):
    consumer = MagicMock()
    first, second = _message({"a": 1}, offset=1), _message({"a": 2}, offset=2)
    consumer.consume.side_effect = [[first, second], []]
    ingester = _opened(consumer, tmp_path)

    records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [{"a": 1}, {"a": 2}]
    assert [(record.topic, record.partition, record.offset) for record in records] == [
        ("evt", 0, 1),
        ("evt", 0, 2),
    ]
    assert consumer.consume.call_args.kwargs["num_messages"] == 1000
    assert len(ingester._contexts) == 1


def test_polling_stops_once_the_poll_max_secs_elapse(tmp_path):
    consumer = MagicMock()
    consumer.consume.return_value = [_message({"a": 1})]
    ingester = _opened(consumer, tmp_path, poll_max_secs=0)

    assert list(ingester.poll_changes(consumer)) == []
    consumer.consume.assert_not_called()


def _broker_error(fatal: bool = False):
    error = MagicMock()
    error.fatal.return_value = fatal
    error.__str__ = lambda self: "boom"
    return _message(error=error)


def test_a_broker_error_is_logged_and_polling_goes_on(tmp_path, caplog):
    consumer = MagicMock()
    good = _message({"a": 1})
    consumer.consume.side_effect = [[_broker_error(), good], []]
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")

    with caplog.at_level("WARNING"):
        records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [{"a": 1}]
    producer.produce.assert_not_called()
    assert "boom" in caplog.text


def test_a_fatal_broker_error_fails_the_run(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_broker_error(fatal=True)], []]
    ingester = _opened(consumer, tmp_path)

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))
    assert raised.value.api_error is KafkaErrorCode.KAFKA_26


def test_undecodable_messages_go_to_the_dlq(tmp_path):
    consumer = MagicMock()
    garbled = _message(raw=b"not json")
    good = _message({"a": 1})
    consumer.consume.side_effect = [[garbled, good], []]
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")

    records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [None, {"a": 1}]
    assert records[0].offset == garbled.offset()
    assert producer.produce.call_count == 1
    assert producer.produce.call_args.kwargs["topic"] == "evt_dlq"


def test_a_dead_letter_message_leaves_a_position_only_record(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad", offset=6, partition=2)], []]
    ingester, _ = _with_dlq(consumer, tmp_path)

    (record,) = ingester.poll_changes(consumer)
    encoder = _encoder()

    assert record.value is None and record.schema_id is None
    assert encoder.slot(record) is None
    assert encoder.offset(record).offsets == {"evt": {2: 7}}


def _decoding(ingester: KafkaIngester, outcomes: list):
    def deserialize(_value, _ctx):
        outcome = outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome, ("a",)

    ingester._serde.deserializer_with_schema_id = lambda: deserialize


def test_a_registry_outage_stops_the_stream_without_committing(tmp_path):
    from confluent_kafka.schema_registry.error import SchemaRegistryError

    consumer = MagicMock()
    consumer.consume.side_effect = [[_message({"a": 1}, offset=7, partition=2)], []]
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")
    _decoding(ingester, [SchemaRegistryError(503, 50301, "unavailable")])

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))

    assert raised.value.api_error is KafkaErrorCode.KAFKA_27
    assert "offset 7" in str(raised.value) and "partition 2" in str(raised.value)
    producer.produce.assert_not_called()
    consumer.commit.assert_not_called()


def test_a_glue_resolution_failure_is_retryable(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message({"a": 1})], []]
    ingester = _opened(consumer, tmp_path)
    _decoding(
        ingester,
        [KafkaErrorCode.KAFKA_18.exception(registry_name="r", schema_name="s")],
    )

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))
    assert raised.value.api_error is KafkaErrorCode.KAFKA_27


def test_a_wrapped_network_error_is_retryable(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message({"a": 1})], []]
    ingester = _opened(consumer, tmp_path)
    wrapped = ValueError("decode failed")
    wrapped.__cause__ = ConnectionError("registry unreachable")
    _decoding(ingester, [wrapped])

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))
    assert raised.value.api_error is KafkaErrorCode.KAFKA_27


def test_an_undecodable_message_without_a_dlq_stops_the_stream(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"not json", offset=3)], []]
    ingester = _opened(consumer, tmp_path)

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))

    assert raised.value.api_error is KafkaErrorCode.KAFKA_28
    assert "offset 3" in str(raised.value)
    consumer.commit.assert_not_called()


def test_a_schema_violation_is_a_format_error_for_the_dlq(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message({"a": 1}), _message({"a": 2})], []]
    producer = MagicMock()
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")
    _decoding(
        ingester,
        [KafkaErrorCode.KAFKA_15.exception(error="id is required"), {"a": 2}],
    )

    records = list(ingester.poll_changes(consumer))

    assert [record.value for record in records] == [None, {"a": 2}]
    assert producer.produce.call_count == 1


def test_the_broker_keeps_the_offset():
    ingester = _ingester()
    assert ingester.resume_offset(first_stage=False, stream_state={"x": 1}) is None
    assert ingester.base_offset().offsets == {}


def _committed(consumer) -> list[tuple[str, int, int]]:
    (call,) = consumer.commit.call_args_list
    assert call.kwargs["asynchronous"] is False
    return [(tp.topic, tp.partition, tp.offset) for tp in call.kwargs["offsets"]]


def test_the_stage_state_acknowledges_every_partition_staged(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    position = KafkaOffset({"evt": {0: 5, 2: 3}})

    ingester.stage_state(position)()

    assert _committed(consumer) == [("evt", 0, 5), ("evt", 2, 3)]


def test_the_stage_state_acknowledges_what_was_staged_when_it_was_taken(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    position = KafkaOffset({"evt": {0: 2}})

    commit = ingester.stage_state(position)
    position.update(KafkaOffset({"evt": {0: 3, 1: 1}}))
    commit()

    assert _committed(consumer) == [("evt", 0, 2)]


def test_a_stage_state_without_offsets_acknowledges_nothing(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    ingester.stage_state(KafkaOffset())()
    consumer.commit.assert_not_called()


def test_only_the_partitions_still_owned_are_acknowledged(tmp_path):
    from confluent_kafka import TopicPartition

    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    consumer.assignment.return_value = [TopicPartition("evt", 2)]

    ingester.stage_state(KafkaOffset({"evt": {0: 5, 2: 3}}))()

    assert _committed(consumer) == [("evt", 2, 3)]


def test_nothing_is_acknowledged_when_no_partition_is_owned_any_more(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    consumer.assignment.return_value = []
    ingester.stage_state(KafkaOffset({"evt": {0: 5}}))()
    consumer.commit.assert_not_called()


def test_a_retriable_acknowledgement_error_is_retried(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    ingester.offsets.sleep = MagicMock()
    consumer.commit.side_effect = [_retriable("broker away"), None]

    ingester.stage_state(KafkaOffset({"evt": {0: 5}}))()

    assert consumer.commit.call_count == 2
    ingester.offsets.sleep.assert_called_once_with(1.0)


def test_a_final_acknowledgement_error_is_not_retried(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    consumer.commit.side_effect = _final("not a member")

    with pytest.raises(TabsdataException) as raised:
        ingester.stage_state(KafkaOffset({"evt": {0: 5}}))()

    assert raised.value.api_error is KafkaErrorCode.KAFKA_30
    assert consumer.commit.call_count == 1


def test_an_acknowledgement_that_keeps_failing_stops_the_stream(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    ingester.offsets.sleep = MagicMock()
    consumer.commit.side_effect = _retriable("broker away")

    with pytest.raises(TabsdataException) as raised:
        ingester.stage_state(KafkaOffset({"evt": {0: 5}}))()

    assert raised.value.api_error is ConnErrorCode.CONN_35
    assert consumer.commit.call_count == 3


def test_the_subscription_listens_for_revoked_and_lost_partitions(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    kwargs = consumer.subscribe.call_args.kwargs
    assert consumer.subscribe.call_args.args == (["evt"],)
    assert kwargs["on_revoke"] == ingester.partitions_revoked
    assert kwargs["on_lost"] == ingester.partitions_lost


def test_a_revocation_settles_everything_pending(tmp_path):
    from confluent_kafka import TopicPartition

    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    lifecycle = Settling()
    ingester.attach_lifecycle(lifecycle)

    ingester.partitions_revoked(consumer, [TopicPartition("evt", 1)])

    assert lifecycle.settled == 1


def test_a_revocation_while_closing_settles_nothing(tmp_path):
    from confluent_kafka import TopicPartition

    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)
    lifecycle = Settling()
    ingester.attach_lifecycle(lifecycle)
    consumer.close.side_effect = lambda: ingester.partitions_revoked(
        consumer, [TopicPartition("evt", 1)]
    )

    ingester.close_session(consumer)

    assert lifecycle.settled == 0


def test_losing_partitions_restarts_the_stream_at_the_next_poll(tmp_path):
    from confluent_kafka import TopicPartition

    consumer = MagicMock()
    consumer.consume.return_value = []
    ingester = _opened(consumer, tmp_path)
    lifecycle = Settling()
    ingester.attach_lifecycle(lifecycle)

    ingester.partitions_lost(consumer, [TopicPartition("evt", 1)])
    with pytest.raises(StreamRestart) as raised:
        list(ingester.poll_changes(consumer))

    assert "('evt', 1)" in raised.value.reason
    assert lifecycle.settled == 0
    assert list(ingester.poll_changes(consumer)) == []


def test_a_long_gap_between_polls_is_warned_about(tmp_path, caplog):
    consumer = MagicMock()
    consumer.consume.return_value = []
    ingester = _opened(consumer, tmp_path)
    ingester._polled_at = time.time() - 200

    with caplog.at_level("WARNING"):
        list(ingester.poll_changes(consumer))

    assert "200 seconds without polling" in caplog.text
    assert "max.poll.interval.ms" in caplog.text


def test_a_short_gap_between_polls_is_quiet(tmp_path, caplog):
    consumer = MagicMock()
    consumer.consume.return_value = []
    ingester = _opened(consumer, tmp_path)
    ingester._polled_at = time.time() - 100

    with caplog.at_level("WARNING"):
        list(ingester.poll_changes(consumer))

    assert "without polling" not in caplog.text


def test_the_poll_gap_warning_follows_the_interval_given(tmp_path, caplog):
    consumer = MagicMock()
    consumer.consume.return_value = []
    ingester = _opened(consumer, tmp_path, conn_cfg={"max.poll.interval.ms": 100_000})
    ingester._polled_at = time.time() - 60

    with caplog.at_level("WARNING"):
        list(ingester.poll_changes(consumer))

    assert "60 seconds without polling, more than half of the 100 seconds" in (
        caplog.text
    )


def _with_dlq(consumer, tmp_path) -> tuple[KafkaIngester, MagicMock]:
    producer = MagicMock()
    producer.flush.return_value = 0
    with patch("confluent_kafka.Producer", return_value=producer):
        ingester = _opened(consumer, tmp_path, dlq_topic="evt_dlq")
    return ingester, producer


def test_dead_letter_messages_are_produced_without_a_flush_per_message(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad"), _message(raw=b"worse")], []]
    ingester, producer = _with_dlq(consumer, tmp_path)

    assert [r.value for r in ingester.poll_changes(consumer)] == [None, None]

    assert producer.produce.call_count == 2
    assert producer.poll.call_count == 2
    producer.flush.assert_not_called()
    assert callable(producer.produce.call_args.kwargs["on_delivery"])


def test_a_full_producer_queue_is_flushed_once_and_the_message_retried(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad")], []]
    ingester, producer = _with_dlq(consumer, tmp_path)
    producer.produce.side_effect = [BufferError("queue full"), None]

    assert [r.value for r in ingester.poll_changes(consumer)] == [None]

    assert producer.produce.call_count == 2
    producer.flush.assert_called_once_with(16)


def test_a_producer_queue_that_stays_full_stops_the_stream(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad")], []]
    ingester, producer = _with_dlq(consumer, tmp_path)
    producer.produce.side_effect = BufferError("queue full")
    producer.__len__.return_value = 7

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))

    assert raised.value.api_error is KafkaErrorCode.KAFKA_29
    assert isinstance(raised.value.__cause__, BufferError)


def test_the_commit_settles_the_dead_letter_topic_before_committing(tmp_path):
    consumer = MagicMock()
    ingester, producer = _with_dlq(consumer, tmp_path)
    order = []
    producer.flush.side_effect = lambda timeout: order.append(("flush", timeout)) or 0
    consumer.commit.side_effect = lambda **_: order.append(("commit", None))

    ingester.stage_state(KafkaOffset({"evt": {0: 2}}))()

    assert order == [("flush", 16), ("commit", None)]


def test_an_undelivered_dead_letter_message_stops_the_commit(tmp_path):
    consumer = MagicMock()
    ingester, producer = _with_dlq(consumer, tmp_path)
    producer.flush.return_value = 1

    with pytest.raises(TabsdataException) as raised:
        ingester.stage_state(KafkaOffset({"evt": {0: 2}}))()

    assert raised.value.api_error is KafkaErrorCode.KAFKA_29
    consumer.commit.assert_not_called()


def test_a_rejected_record_is_dead_lettered_with_its_decoded_value(tmp_path):
    consumer = MagicMock()
    ingester, producer = _with_dlq(consumer, tmp_path)
    record = _record({"a": "x"}, offset=4, partition=1)

    ingester.reject_record(record, ValueError("not an integer"))

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == "evt_dlq"
    envelope = json.loads(kwargs["value"])
    assert envelope["record"] == {"a": "x"}
    assert envelope["original_value"] is None
    assert (envelope["topic"], envelope["partition"], envelope["offset"]) == ("evt", 1, 4)
    assert "not an integer" in envelope["error"]


def test_a_rejected_record_without_a_dlq_stops_the_stream(tmp_path):
    consumer = MagicMock()
    ingester = _opened(consumer, tmp_path)

    with pytest.raises(TabsdataException) as raised:
        ingester.reject_record(_record({"a": "x"}, offset=4), ValueError("bad"))

    assert raised.value.api_error is KafkaErrorCode.KAFKA_31
    assert "offset 4 of partition 0 of topic 'evt'" in str(raised.value)


def test_a_failed_dead_letter_delivery_stops_the_stream_at_once(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad", offset=9)], []]
    ingester, producer = _with_dlq(consumer, tmp_path)
    producer.produce.side_effect = lambda **kwargs: kwargs["on_delivery"](
        "Unknown topic", None
    )

    with pytest.raises(TabsdataException) as raised:
        list(ingester.poll_changes(consumer))

    assert raised.value.api_error is KafkaErrorCode.KAFKA_29
    assert "offset 9 of partition 0 of topic 'evt': Unknown topic" in str(
        raised.value
    )


def test_a_failed_dead_letter_delivery_stops_the_commit(tmp_path):
    consumer = MagicMock()
    consumer.consume.side_effect = [[_message(raw=b"bad", offset=9)], []]
    ingester, producer = _with_dlq(consumer, tmp_path)
    callbacks = []
    producer.produce.side_effect = lambda **kwargs: callbacks.append(
        kwargs["on_delivery"]
    )
    producer.flush.side_effect = lambda _timeout: [
        callback("Unknown topic", None) for callback in callbacks
    ] and 0

    assert [r.value for r in ingester.poll_changes(consumer)] == [None]
    with pytest.raises(TabsdataException) as raised:
        ingester.stage_state(KafkaOffset({"evt": {0: 10}}))()

    assert raised.value.api_error is KafkaErrorCode.KAFKA_29
    assert "offset 9 of partition 0 of topic 'evt': Unknown topic" in str(
        raised.value
    )
    consumer.commit.assert_not_called()


def test_closing_the_session_closes_the_consumer_and_flushes_the_dlq(tmp_path):
    consumer = MagicMock()
    ingester, producer = _with_dlq(consumer, tmp_path)
    ingester.close_session(consumer)
    consumer.close.assert_called_once()
    producer.flush.assert_called_once_with(16)


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


def test_build_kafka_configs_rebalances_cooperatively_by_default():
    cfg = build_kafka_configs(conn=KafkaStreamSrcConn(servers="a:9092", group_id="g"))
    assert cfg["partition.assignment.strategy"] == "cooperative-sticky"


def test_build_kafka_configs_sets_the_connector_defaults():
    cfg = build_kafka_configs(conn=KafkaStreamSrcConn(servers="a:9092", group_id="g"))
    assert cfg["topic.metadata.refresh.interval.ms"] == 30_000
    assert cfg["fetch.queue.backoff.ms"] == 0
    assert cfg["socket.keepalive.enable"] is True


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("topic.metadata.refresh.interval.ms", 5_000),
        ("fetch.queue.backoff.ms", 500),
        ("socket.keepalive.enable", False),
    ],
)
def test_build_kafka_configs_keeps_the_values_given_over_its_defaults(key, value):
    conn = KafkaStreamSrcConn(servers="a:9092", group_id="g", conn_cfg={key: value})
    assert build_kafka_configs(conn=conn)[key] == value


def test_build_kafka_configs_keeps_the_assignment_strategy_given():
    conn = KafkaStreamSrcConn(
        servers="a:9092",
        group_id="g",
        conn_cfg={"partition.assignment.strategy": "range"},
    )
    assert build_kafka_configs(conn=conn)["partition.assignment.strategy"] == "range"


def test_build_kafka_configs_conn_cfg_overrides_pass_through():
    conn = KafkaStreamSrcConn(
        servers="a:9092",
        group_id="g1",
        conn_cfg={"session.timeout.ms": 30000},
    )
    cfg = build_kafka_configs(conn=conn)
    assert cfg["session.timeout.ms"] == 30000

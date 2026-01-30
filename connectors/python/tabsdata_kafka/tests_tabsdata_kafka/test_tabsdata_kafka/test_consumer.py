#
# Copyright 2026 Tabs Data Inc.
#

import logging
import time
import uuid

import pytest
from confluent_kafka import Consumer

from tabsdata_kafka.consumer.consumer import KafkaConsumer, RolloverReason
from tabsdata_kafka.consumer.messages_buffer import MessagesBuffer
from tests_tabsdata.common.backend.kafka import kafka_config
from tests_tabsdata_kafka.conftest import (
    get_avro_message,
    get_json_message,
    get_large_avro_message,
    get_large_json_message,
    get_large_protobuf_message,
    get_protobuf_message,
    produce_kafka_messages,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

TIMEOUT_SECS = 10


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_add_and_get_messages():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.message_count == 0

    assert buffer.messages == []

    buffer.append((10, {"key": "value1"}, "schema1"))

    assert buffer.message_count == 1

    assert buffer.messages == [{"key": "value1"}]

    buffer.append((10, {"key": "value2"}, "schema1"))

    assert buffer.message_count == 2

    assert buffer.messages == [{"key": "value1"}, {"key": "value2"}]


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_schema_id_compatibility():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.append((10, {"key": "value1"}, "schema1")) is True

    assert buffer.append((10, {"key": "value2"}, "schema1")) is True

    assert buffer.append((10, {"key": "value3"}, "schema2")) is False


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_message_count():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.message_count == 0

    for i in range(5):
        buffer.append((10, {f"key{i}": f"value{i}"}, "schema1"))

    assert buffer.message_count == 5


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_size_mb():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.size_mb == 0

    one_mb = 1024 * 1024

    buffer.append((one_mb, {"key": "value1"}, "schema1"))

    assert buffer.size_mb == 1

    buffer.append((one_mb * 2, {"key": "value2"}, "schema1"))

    assert buffer.size_mb == 3


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_to_parquet(tmp_path):
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.to_parquet(str(tmp_path)) is None

    buffer.append((10, {"col1": 1, "col2": "a"}, "schema1"))

    buffer.append((10, {"col1": 2, "col2": "b"}, "schema1"))

    parquet_file = buffer.to_parquet(str(tmp_path))

    assert parquet_file.exists()

    import pyarrow.parquet as pq

    table = pq.read_table(parquet_file)

    assert table.num_rows == 2

    assert table.num_columns == 2

    assert table.column_names == ["col1", "col2"]

    assert table.to_pydict() == {"col1": [1, 2], "col2": ["a", "b"]}


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_to_parquet_with_missing_columns(tmp_path):
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.to_parquet(str(tmp_path)) is None

    buffer.append((10, {"col1": 1, "col2": "a", "col3": 1.1}, "schema1"))

    buffer.append((10, {"col1": 2, "col2": "b"}, "schema1"))

    parquet_file = buffer.to_parquet(str(tmp_path))

    assert parquet_file.exists()

    import pyarrow.parquet as pq

    table = pq.read_table(parquet_file)

    assert table.num_rows == 2

    assert table.num_columns == 3

    assert table.column_names == ["col1", "col2", "col3"]

    d = table.to_pydict()

    assert d["col1"] == [1, 2]

    assert d["col2"] == ["a", "b"]

    assert d["col3"][0] == 1.1

    assert d["col3"][1] is None


@pytest.mark.unit
@pytest.mark.kafka
@pytest.mark.parametrize(
    "serde_fixture, get_message",
    [
        ("avro_serde_v1", get_avro_message),
        ("json_serde", get_json_message),
        ("protobuf_serde_v1", get_protobuf_message),
        ("confluent_sr_avro_serde", get_avro_message),
        ("confluent_sr_json_serde", get_avro_message),
        ("confluent_sr_protobuf_serde", get_protobuf_message),
        ("aws_glue_sr_avro_glue_serde", get_avro_message),
        ("aws_glue_sr_json_glue_serde", get_avro_message),
        ("aws_glue_sr_protobuf_glue_serde", get_protobuf_message),
    ],
)
def test_consumer_messages_rollover(
    serde_fixture, get_message, request, kafka_config, produce_kafka_messages
):
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    topic = f"test-consumer-messages-rollover-{serde.format}-{str(uuid.uuid4())}"

    total_messages = 100
    produce_kafka_messages(topic, total_messages, serde, get_message)

    # Create consumer
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }
    kafka_consumer = Consumer(consumer_config)
    kafka_consumer.list_topics(timeout=TIMEOUT_SECS)

    try:
        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(topic, kafka_consumer, serde, messages_rollover=10)

        responses = []
        received_messages = 0
        loop_start_time = time.time()
        while received_messages < total_messages:
            if time.time() - loop_start_time > 30:  # 30 second timeout
                pytest.fail(
                    "Timeout waiting for messages in messages_rollover test. "
                    f"Received {received_messages}/{total_messages} messages"
                )

            (reason, buffer) = consumer.consume(1)
            if buffer.message_count > 0:
                received_messages += buffer.message_count
                responses.append((reason, buffer))
                consumer.commit()

        assert total_messages == received_messages

        for reason, buffer in responses:
            assert reason in [
                RolloverReason.Messages,
                RolloverReason.Time,
                RolloverReason.SchemaChange,
            ]
            assert buffer.message_count <= 10
            if reason == RolloverReason.Messages:
                assert buffer.message_count == 10

    finally:
        kafka_consumer.close()


@pytest.mark.unit
@pytest.mark.kafka
@pytest.mark.parametrize(
    "serde_fixture, get_message",
    [
        ("avro_serde_v1", get_avro_message),
        ("json_serde", get_json_message),
        ("protobuf_serde_v1", get_protobuf_message),
        ("confluent_sr_avro_serde", get_avro_message),
        ("confluent_sr_json_serde", get_avro_message),
        ("confluent_sr_protobuf_serde", get_protobuf_message),
        ("aws_glue_sr_avro_glue_serde", get_avro_message),
        ("aws_glue_sr_json_glue_serde", get_avro_message),
        ("aws_glue_sr_protobuf_glue_serde", get_protobuf_message),
    ],
)
def test_consumer_time_rollover(
    serde_fixture, get_message, request, kafka_config, produce_kafka_messages
):
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    topic = f"test-consumer-time-rollover-{serde.format}-{str(uuid.uuid4())}"

    sent_messages = 5
    produce_kafka_messages(topic, sent_messages, serde, get_message)

    # Create consumer
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }
    kafka_consumer = Consumer(consumer_config)
    kafka_consumer.list_topics(timeout=TIMEOUT_SECS)

    try:
        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(topic, kafka_consumer, serde)

        received_messages = 0
        loop_start_time = time.time()
        while received_messages < sent_messages:
            if time.time() - loop_start_time > 10:  # 10 second timeout for the loop
                pytest.fail("Timeout waiting for messages in time_rollover test")

            (reason, buffer) = consumer.consume(2, reset_buffer=True)
            if buffer.message_count > 0:
                assert reason in [RolloverReason.Time, RolloverReason.SchemaChange]
                received_messages += buffer.message_count
                consumer.commit()

        assert received_messages == sent_messages

        # Second consume should timeout and return no messages
        start_time = time.time()
        (reason, buffer) = consumer.consume(3, reset_buffer=True)
        end_time = time.time()

        assert reason == RolloverReason.Time
        assert buffer.message_count == 0
        assert (
            end_time - start_time >= 2
        )  # checking for 2 instead of 3 due to some fractions randomness

    finally:
        kafka_consumer.close()


@pytest.mark.unit
@pytest.mark.kafka
@pytest.mark.parametrize(
    "serde_fixture, get_message",
    [
        ("avro_serde_v1", get_large_avro_message),
        ("json_serde", get_large_json_message),
        ("protobuf_serde_v1", get_large_protobuf_message),
        ("confluent_sr_avro_serde", get_large_avro_message),
        ("confluent_sr_json_serde", get_large_avro_message),
        ("confluent_sr_protobuf_serde", get_large_protobuf_message),
        ("aws_glue_sr_avro_glue_serde", get_large_avro_message),
        ("aws_glue_sr_json_glue_serde", get_large_avro_message),
        ("aws_glue_sr_protobuf_glue_serde", get_large_protobuf_message),
    ],
)
def test_consumer_size_rollover(
    serde_fixture, get_message, request, kafka_config, produce_kafka_messages
):
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    topic = f"test-consumer-size-rollover-{serde.format}-{str(uuid.uuid4())}"

    sent_messages = 1100
    produce_kafka_messages(topic, sent_messages, serde, get_message)

    # Create consumer
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": str(uuid.uuid4()),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": kafka_config.user,
        "sasl.password": kafka_config.password,
    }
    kafka_consumer = Consumer(consumer_config)
    kafka_consumer.list_topics(timeout=TIMEOUT_SECS)

    try:
        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(topic, kafka_consumer, serde, size_rollover_mb=1)

        responses = []
        received_messages = 0
        first_rollover = False
        loop_start_time = time.time()
        while received_messages < sent_messages:
            if time.time() - loop_start_time > 60:  # 60 second timeout (larger dataset)
                pytest.fail(
                    "Timeout waiting for messages in size_rollover test. "
                    f"Received {received_messages}/{sent_messages} messages"
                )

            (reason, buffer) = consumer.consume(5)
            if buffer.message_count > 0:
                received_messages += buffer.message_count
                responses.append((reason, buffer))
                consumer.commit()
                if not first_rollover:
                    assert reason in [RolloverReason.Size, RolloverReason.SchemaChange]
                    first_rollover = True
            else:
                break

        assert first_rollover
        assert sent_messages == received_messages

    finally:
        kafka_consumer.close()

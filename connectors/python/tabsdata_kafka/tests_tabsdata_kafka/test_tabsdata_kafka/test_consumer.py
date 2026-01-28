#
# Copyright 2026 Tabs Data Inc.
#

import inspect
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from unittest import mock

import polars as pl
import pyarrow.parquet as pq
import pytest
from confluent_kafka.serialization import MessageField, SerializationContext

import tabsdata as td
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata_kafka._connector import KafkaConn, KafkaStageTrigger
from tabsdata_kafka.consumer.consumer import KafkaConsumer
from tabsdata_kafka.consumer.messages_buffer import MessagesBuffer
from tabsdata_kafka.consumer.rollovers import (
    MessagesRollover,
    RolloverReason,
    SizeRollover,
)
from tabsdata_kafka.serdes.aws_glue_sr_serde import (
    AvroGlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)
from tabsdata_kafka.serdes.confluent_sr_serde import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
)
from tabsdata_kafka.serdes.non_sr_serde import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    ProtobufKafkaSerde,
)
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    write_v1_listener_yaml_file,
)
from tests_tabsdata.fntest import tdfnrun
from tests_tabsdata_kafka.conftest import (
    aws_glue_sr_avro_schema_str,
    aws_glue_sr_json_schema_str,
    aws_glue_sr_mock_avro_glue_client,
    aws_glue_sr_mock_json_glue_client,
    aws_glue_sr_mock_protobuf_glue_client,
    aws_glue_sr_protobuf_schema_str,
    confluent_sr_mock_registry,
    delivery_callback,
    get_avro_message,
    get_json_message,
    get_large_avro_message,
    get_large_json_message,
    get_large_protobuf_message,
    get_protobuf_message,
    json_serde,
    kafka_consumer_config,
    kafka_producer,
    kafka_topic_creator,
    produce_kafka_messages,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_add_and_get_messages():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.message_count() == 0

    assert buffer.messages() == []

    buffer.append((10, {"key": "value1"}, "schema1"))

    assert buffer.message_count() == 1

    assert buffer.messages() == [{"key": "value1"}]

    buffer.append((10, {"key": "value2"}, "schema1"))

    assert buffer.message_count() == 2

    assert buffer.messages() == [{"key": "value1"}, {"key": "value2"}]


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

    assert buffer.message_count() == 0

    for i in range(5):
        buffer.append((10, {f"key{i}": f"value{i}"}, "schema1"))

    assert buffer.message_count() == 5


@pytest.mark.unit
@pytest.mark.kafka
def test_message_buffer_size_mb():
    def schema_ids_compatibles_checker(schema1, schema2):
        return schema1 == schema2

    buffer = MessagesBuffer("test-topic", schema_ids_compatibles_checker)

    assert buffer.size_mb() == 0

    one_mb = 1024 * 1024

    buffer.append((one_mb, {"key": "value1"}, "schema1"))

    assert buffer.size_mb() == 1

    buffer.append((one_mb * 2, {"key": "value2"}, "schema1"))

    assert buffer.size_mb() == 3


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


@pytest.mark.skip(reason="test not using kafka_config docker fixture")
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
    serde_fixture,
    get_message,
    request,
    produce_kafka_messages,
    kafka_consumer,
):
    serde = request.getfixturevalue(serde_fixture)
    try:
        topic = f"test-consumer-messages-rollover-{serde.format()}-{str(uuid.uuid4())}"

        sent_messages = 100
        produce_kafka_messages(topic, sent_messages + 1, serde, get_message)

        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(
            topic, kafka_consumer, serde, messages_rollover=MessagesRollover(10)
        )

        responses = []
        received_messages = 0
        while received_messages < sent_messages:
            (reason, buffer) = consumer.consume(1)
            if buffer:
                received_messages += buffer.message_count()
                responses.append((reason, buffer))
                consumer.commit()

        assert sent_messages == received_messages

        for reason, buffer in responses:
            assert reason in [
                RolloverReason.Messages,
                RolloverReason.Time,
                RolloverReason.SchemaChange,
            ]
            assert buffer.message_count() <= 10
            if reason == RolloverReason.Messages:
                assert buffer.message_count() == 10

    finally:
        kafka_consumer.close()


@pytest.mark.skip(reason="test not using kafka_config docker fixture")
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
    serde_fixture,
    get_message,
    request,
    produce_kafka_messages,
    kafka_consumer,
):
    serde = request.getfixturevalue(serde_fixture)
    try:
        topic = f"test-consumer-time-rollover-{serde.format()}-{str(uuid.uuid4())}"

        # Send 5 messages, less than the message rollover count
        sent_messages = 5
        produce_kafka_messages(topic, sent_messages + 1, serde, get_message)
        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(topic, kafka_consumer, serde)

        received_messages = 0
        loop_start_time = time.time()
        while received_messages < sent_messages:
            if time.time() - loop_start_time > 10:  # 10 second timeout for the loop
                pytest.fail("Timeout waiting for messages in time_rollover test")

            (reason, buffer) = consumer.consume(2, reset_buffer=True)
            if buffer.message_count() > 0:
                assert reason in [RolloverReason.Time, RolloverReason.SchemaChange]
                received_messages += buffer.message_count()
                consumer.commit()

        assert received_messages == sent_messages

        # Second consume should timeout and return no messages
        start_time = time.time()
        (reason, buffer) = consumer.consume(3, reset_buffer=True)
        end_time = time.time()

        assert reason == RolloverReason.Time
        assert buffer.message_count() == 0
        assert (
            end_time - start_time >= 2
        )  # checking for 2 instead of 3 due to some fractions randomness

    finally:
        kafka_consumer.close()


@pytest.mark.skip(reason="test not using kafka_config docker fixture")
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
    serde_fixture,
    get_message,
    request,
    produce_kafka_messages,
    kafka_consumer,
):
    serde = request.getfixturevalue(serde_fixture)
    try:
        topic = f"test-consumer-size-rollover-{serde.format()}-{str(uuid.uuid4())}"

        sent_messages = 1100
        produce_kafka_messages(topic, sent_messages + 1, serde, get_message)

        kafka_consumer.subscribe([topic])
        consumer = KafkaConsumer(
            topic, kafka_consumer, serde, size_rollover=SizeRollover(1)
        )

        responses = []
        received_messages = 0
        first_rollover = False
        while received_messages < sent_messages:
            (reason, buffer) = consumer.consume(5)
            if buffer and buffer.message_count() > 0:
                received_messages += buffer.message_count()
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

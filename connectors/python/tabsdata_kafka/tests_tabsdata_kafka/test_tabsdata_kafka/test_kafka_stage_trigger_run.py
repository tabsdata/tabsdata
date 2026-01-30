#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import cast

import pyarrow.parquet as pq
import pytest

import tabsdata as td
from tabsdata._credentials import UserPasswordCredentials
from tabsdata_kafka._connector import KafkaConn, KafkaTrigger
from tabsdata_kafka.typing import Protobuf
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.common.backend.kafka import kafka_config
from tests_tabsdata.conftest import ABSOLUTE_TEST_FOLDER_LOCATION
from tests_tabsdata.fntest import tdfnjob
from tests_tabsdata_kafka.conftest import (
    PROTOBUF_MESSAGE_NAME,
    TIMEOUT_SECS,
    get_avro_message,
    get_json_message,
    get_large_avro_message,
    get_large_json_message,
    get_large_protobuf_message,
    get_protobuf_message,
    kafka_consumer_fixture,
)

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)

EDGE_OF_TIME: float = 0

TOTAL_MESSAGES = 64
TOTAL_BATCHES = 2
BATCH_MESSAGES = TOTAL_MESSAGES // TOTAL_BATCHES
KAFKA_TRIGGER_DURATION = 16

RESPONSE_FOLDER = "response_folder"


LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr",
    [
        (
            "avro_serde_v1",
            get_avro_message,
            None,
        ),
        (
            "json_serde",
            get_json_message,
            None,
        ),
        (
            "protobuf_serde_v1",
            get_protobuf_message,
            None,
        ),
        (
            "confluent_sr_avro_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_json_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_protobuf_serde",
            get_protobuf_message,
            "confluent_schema_registry",
        ),
        (
            "aws_glue_sr_avro_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_json_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_protobuf_glue_serde",
            get_protobuf_message,
            "aws_glue_schema_registry",
        ),
    ],
)
def test_kafka_stage_trigger_time_rollover(
    serde_fixture,
    get_message,
    get_sr,
    request,
    kafka_config,
    produce_kafka_messages,
):
    topic = str(uuid.uuid4())
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    sr = request.getfixturevalue(get_sr)(kafka_config, serde) if get_sr else None

    produce_kafka_messages(topic, TOTAL_MESSAGES, serde, get_message)

    if serde.format == "protobuf":
        data_format = Protobuf(PROTOBUF_MESSAGE_NAME)
    else:
        data_format = serde.format

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
            schema_registry=sr,
        ),
        topic=topic,
        data_format=data_format,
        schema=serde._schema,
        time_rollover_secs=5,
        end=datetime.now(timezone.utc) + timedelta(seconds=KAFKA_TRIGGER_DURATION),
    )

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=tfr,
        function_defined_on=EDGE_OF_TIME,
    )
    parquet_files = fn_trigger.output.as_os_fd()
    assert len(parquet_files) == TOTAL_BATCHES - 1
    parquet_files_sorted = sorted(parquet_files, key=lambda f: f.stat().st_ctime)
    for i, parquet_file in enumerate(parquet_files_sorted):
        rows = pq.read_table(parquet_file).num_rows
        assert rows == TOTAL_MESSAGES


@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr",
    [
        (
            "avro_serde_v1",
            get_avro_message,
            None,
        ),
        (
            "json_serde",
            get_json_message,
            None,
        ),
        (
            "protobuf_serde_v1",
            get_protobuf_message,
            None,
        ),
        (
            "confluent_sr_avro_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_json_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_protobuf_serde",
            get_protobuf_message,
            "confluent_schema_registry",
        ),
        (
            "aws_glue_sr_avro_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_json_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_protobuf_glue_serde",
            get_protobuf_message,
            "aws_glue_schema_registry",
        ),
    ],
)
def test_kafka_stage_trigger_time_rollover_w_dlq(
    serde_fixture,
    get_message,
    get_sr,
    request,
    kafka_config,
    produce_kafka_messages,
    kafka_producer,
    kafka_topic_creator,
    kafka_consumer_fixture,
):
    topic = str(uuid.uuid4())
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    sr = request.getfixturevalue(get_sr)(kafka_config, serde) if get_sr else None

    dlq_topic = f"{uuid.uuid4()}_dlq"

    kafka_topic_creator(dlq_topic)

    # Send a bad message (invalid for the serde)
    for _ in range(BATCH_MESSAGES):
        bad_value = b"this is not valid for the expected schema"
        kafka_producer.produce(topic, value=bad_value)
        kafka_producer.list_topics(timeout=TIMEOUT_SECS)
        kafka_producer.poll(0)
        kafka_producer.flush()

    produce_kafka_messages(topic, TOTAL_MESSAGES, serde, get_message)

    if serde.format == "protobuf":
        data_format = Protobuf(PROTOBUF_MESSAGE_NAME)
    else:
        data_format = serde.format

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
            schema_registry=sr,
        ),
        topic=topic,
        data_format=data_format,
        schema=serde._schema,
        time_rollover_secs=5,
        end=datetime.now(timezone.utc) + timedelta(seconds=KAFKA_TRIGGER_DURATION),
        dlq_topic=dlq_topic,
    )

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=tfr,
        function_defined_on=EDGE_OF_TIME,
    )
    parquet_files = fn_trigger.output.as_os_fd()
    assert len(parquet_files) == TOTAL_BATCHES - 1
    parquet_files_sorted = sorted(parquet_files, key=lambda f: f.stat().st_ctime)
    for i, parquet_file in enumerate(parquet_files_sorted):
        rows = pq.read_table(parquet_file).num_rows
        assert rows == TOTAL_MESSAGES

    dlq_consumer = kafka_consumer_fixture
    dlq_consumer.subscribe([dlq_topic])
    dlq_consumer.list_topics(timeout=TIMEOUT_SECS)

    dlq_messages = []
    while len(dlq_messages) < BATCH_MESSAGES:
        msg = dlq_consumer.poll(timeout=TIMEOUT_SECS)
        if msg is None:
            break  # No more messages
        if msg.error():
            continue
        dlq_messages.append(msg.value())
    dlq_consumer.close()

    assert len(dlq_messages) == BATCH_MESSAGES


@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr",
    [
        (
            "avro_serde_v1",
            get_avro_message,
            None,
        ),
        (
            "json_serde",
            get_json_message,
            None,
        ),
        (
            "protobuf_serde_v1",
            get_protobuf_message,
            None,
        ),
        (
            "confluent_sr_avro_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_json_serde",
            get_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_protobuf_serde",
            get_protobuf_message,
            "confluent_schema_registry",
        ),
        (
            "aws_glue_sr_avro_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_json_glue_serde",
            get_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_protobuf_glue_serde",
            get_protobuf_message,
            "aws_glue_schema_registry",
        ),
    ],
)
def test_kafka_stage_trigger_message_rollover(
    serde_fixture,
    get_message,
    get_sr,
    request,
    kafka_config,
    produce_kafka_messages,
):
    topic = str(uuid.uuid4())
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    sr = request.getfixturevalue(get_sr)(kafka_config, serde) if get_sr else None
    produce_kafka_messages(topic, TOTAL_MESSAGES, serde, get_message)

    if serde.format == "protobuf":
        data_format = Protobuf(PROTOBUF_MESSAGE_NAME)
    else:
        data_format = serde.format

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
            schema_registry=sr,
        ),
        topic=topic,
        data_format=data_format,
        schema=serde._schema,
        messages_rollover=BATCH_MESSAGES,
        end=datetime.now(timezone.utc) + timedelta(seconds=KAFKA_TRIGGER_DURATION),
    )

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=tfr,
        function_defined_on=EDGE_OF_TIME,
    )

    parquet_files = fn_trigger.output.as_os_fd()
    assert len(parquet_files) == TOTAL_BATCHES
    parquet_files_sorted = sorted(parquet_files, key=lambda f: f.stat().st_ctime)
    for i, parquet_file in enumerate(parquet_files_sorted):
        rows = pq.read_table(parquet_file).num_rows
        assert rows == BATCH_MESSAGES


@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr",
    [
        (
            "avro_serde_v1",
            get_large_avro_message,
            None,
        ),
        (
            "json_serde",
            get_large_json_message,
            None,
        ),
        (
            "protobuf_serde_v1",
            get_large_protobuf_message,
            None,
        ),
        (
            "confluent_sr_avro_serde",
            get_large_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_json_serde",
            get_large_avro_message,
            "confluent_schema_registry",
        ),
        (
            "confluent_sr_protobuf_serde",
            get_large_protobuf_message,
            "confluent_schema_registry",
        ),
        (
            "aws_glue_sr_avro_glue_serde",
            get_large_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_json_glue_serde",
            get_large_avro_message,
            "aws_glue_schema_registry",
        ),
        (
            "aws_glue_sr_protobuf_glue_serde",
            get_large_protobuf_message,
            "aws_glue_schema_registry",
        ),
    ],
)
def test_kafka_stage_trigger_size_rollover(
    serde_fixture,
    get_message,
    get_sr,
    request,
    kafka_config,
    produce_kafka_messages,
):
    topic = str(uuid.uuid4())
    serde_obj = request.getfixturevalue(serde_fixture)
    if callable(serde_obj):
        serde = serde_obj(kafka_config)
    else:
        serde = serde_obj
    sr = request.getfixturevalue(get_sr)(kafka_config, serde) if get_sr else None

    size_rollover_messages = TOTAL_MESSAGES * 20

    produce_kafka_messages(topic, size_rollover_messages, serde, get_message)

    if serde.format == "protobuf":
        data_format = Protobuf(PROTOBUF_MESSAGE_NAME)
    else:
        data_format = serde.format

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
            schema_registry=sr,
        ),
        topic=topic,
        data_format=data_format,
        schema=serde._schema,
        size_rollover_mb=1,
        end=datetime.now(timezone.utc) + timedelta(seconds=KAFKA_TRIGGER_DURATION),
    )

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=tfr,
        function_defined_on=EDGE_OF_TIME,
    )

    parquet_files = fn_trigger.output.as_os_fd()
    assert len(parquet_files) == TOTAL_BATCHES
    parquet_files_sorted = sorted(parquet_files, key=lambda f: f.stat().st_ctime)
    for i, parquet_file in enumerate(parquet_files_sorted):
        size = round(pq.read_table(parquet_file).nbytes / (1024 * 1024))
        if i == 0:
            assert size >= 1
        else:
            assert size <= 1

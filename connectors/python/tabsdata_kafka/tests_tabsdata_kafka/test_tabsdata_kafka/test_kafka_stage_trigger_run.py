#
# Copyright 2026 Tabs Data Inc.
#

import inspect
import logging
import os
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
    get_avro_message,
    get_json_message,
    get_protobuf_message,
    json_serde,
    kafka_consumer_config,
    kafka_producer,
    kafka_topic_creator,
    produce_kafka_messages,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.skip(reason="test not using kafka_config docker fixture")
@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr_client",
    [
        ("avro_serde_v1", get_avro_message, None),
        ("json_serde", get_json_message, None),
        ("protobuf_serde_v1", get_protobuf_message, None),
        # ("confluent_sr_avro_serde", get_avro_message, "confluent_sr_mock_registry"),
        # ("confluent_sr_json_serde", get_avro_message, "confluent_sr_mock_registry"),
        # (
        #     "confluent_sr_protobuf_serde",
        #     get_protobuf_message,
        #     "confluent_sr_mock_registry",
        # ),
        # (
        #     "aws_glue_sr_avro_glue_serde",
        #     get_avro_message,
        #     "aws_glue_sr_mock_avro_glue_client",
        # ),
        # (
        #     "aws_glue_sr_json_glue_serde",
        #     get_avro_message,
        #     "aws_glue_sr_mock_json_glue_client",
        # ),
        # (
        #     "aws_glue_sr_protobuf_glue_serde",
        #     get_protobuf_message,
        #     "aws_glue_sr_mock_protobuf_glue_client",
        # ),
    ],
)
def test_kafka_stage_trigger_time_rollover(
    serde_fixture,
    get_message,
    get_sr_client,
    kafka_consumer_config,
    kafka_topic_creator,
    produce_kafka_messages,
    tmp_path,
    request,
):
    topic = f"test-consumer-messages-rollover-{str(uuid.uuid4())}"

    serde = request.getfixturevalue(serde_fixture)
    sr_client = request.getfixturevalue(get_sr_client) if get_sr_client else None
    sr_client = sr_client[0] if isinstance(sr_client, tuple) else sr_client

    sent_messages = 6
    produce_kafka_messages(topic, sent_messages, serde, get_message)

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        trigger_by=KafkaStageTrigger(
            conn=KafkaConn(kafka_consumer_config),
            topic=topic,
            data_format=serde.format(),
            schema=serde._schema,
            schema_registry_client=sr_client,
            time_rollover_secs=5,
            end=datetime.now() + timedelta(seconds=30),
            message_name=getattr(serde, "_message_name", None),
            registry_name=getattr(serde, "_registry_name", None),
            schema_name=getattr(serde, "_schema_name", None),
        ),
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        new_tfs = td.concat(tfs)
        return new_tfs

    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        tfr, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    staging_folder = tmp_path / "staging"
    staging_uri = staging_folder.as_uri()
    write_v1_listener_yaml_file(
        input_yaml_file,
        context_archive,
        staging_uri=staging_uri,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
        temp_cwd=True,
    )
    assert staging_folder.exists()
    parquet_files = list(staging_folder.glob("*.parquet"))
    assert len(parquet_files) == 1

    output_file = parquet_files[0]
    tfr_result = pl.read_parquet(output_file)
    assert tfr_result.shape[0] == sent_messages - 1


@pytest.mark.skip(reason="test not using kafka_config docker fixture")
@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr_client",
    [
        ("avro_serde_v1", get_avro_message, None),
        ("json_serde", get_json_message, None),
        ("protobuf_serde_v1", get_protobuf_message, None),
        # ("confluent_sr_avro_serde", get_avro_message, "confluent_sr_mock_registry"),
        # ("confluent_sr_json_serde", get_avro_message, "confluent_sr_mock_registry"),
        # (
        #     "confluent_sr_protobuf_serde",
        #     get_protobuf_message,
        #     "confluent_sr_mock_registry",
        # ),
        # (
        #     "aws_glue_sr_avro_glue_serde",
        #     get_avro_message,
        #     "aws_glue_sr_mock_avro_glue_client",
        # ),
        # (
        #     "aws_glue_sr_json_glue_serde",
        #     get_avro_message,
        #     "aws_glue_sr_mock_json_glue_client",
        # ),
        # (
        #     "aws_glue_sr_protobuf_glue_serde",
        #     get_protobuf_message,
        #     "aws_glue_sr_mock_protobuf_glue_client",
        # ),
    ],
)
def test_kafka_stage_trigger_message_rollover(
    serde_fixture,
    get_message,
    get_sr_client,
    kafka_consumer_config,
    kafka_topic_creator,
    produce_kafka_messages,
    tmp_path,
    request,
):
    topic = f"test-consumer-messages-rollover-{str(uuid.uuid4())}"

    serde = request.getfixturevalue(serde_fixture)
    sr_client = request.getfixturevalue(get_sr_client) if get_sr_client else None
    sr_client = sr_client[0] if isinstance(sr_client, tuple) else sr_client

    sent_messages = 6
    produce_kafka_messages(topic, sent_messages, serde, get_message)

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        trigger_by=KafkaStageTrigger(
            conn=KafkaConn(kafka_consumer_config),
            topic=topic,
            data_format=serde.format(),
            schema=serde._schema,
            schema_registry_client=sr_client,
            messages_rollover=MessagesRollover(3),
            end=datetime.now() + timedelta(seconds=30),
            message_name=getattr(serde, "_message_name", None),
            registry_name=getattr(serde, "_registry_name", None),
            schema_name=getattr(serde, "_schema_name", None),
        ),
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:
        new_tfs = td.concat(tfs)
        return new_tfs

    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        tfr, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    staging_folder = tmp_path / "staging"
    staging_uri = staging_folder.as_uri()
    write_v1_listener_yaml_file(
        input_yaml_file,
        context_archive,
        staging_uri=staging_uri,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
        temp_cwd=True,
    )
    assert staging_folder.exists()
    parquet_files = list(staging_folder.glob("*.parquet"))
    assert len(parquet_files) == 2
    parquet_files_sorted = sorted(parquet_files, key=lambda f: f.stat().st_ctime)
    for i, parquet_file in enumerate(parquet_files_sorted):
        num_rows = pq.read_table(parquet_file).num_rows
        if i == 0:
            assert num_rows == 3
        else:
            assert num_rows == 2

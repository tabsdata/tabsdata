#
# Copyright 2026 Tabs Data Inc.
#

import inspect
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import cast
from unittest import mock

import polars as pl
import pyarrow.parquet as pq
import pytest

import tabsdata as td
from tabsdata._credentials import UserPasswordCredentials
from tabsdata._io.stage_trigger import (
    CallConfig,
    StageTriggerStatus,
)
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata_kafka._connector import KafkaConn, KafkaTrigger
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.common.backend.kafka import kafka_config
from tests_tabsdata.common.constants import PYTEST_DEFAULT_ENVIRONMENT_PREFIX
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    write_v2_function_yaml_file,
)
from tests_tabsdata.fntest import tdfnjob
from tests_tabsdata_kafka.conftest import (
    MockLocalFileStageTriggerContext,
    get_avro_message,
    get_json_message,
    get_protobuf_message,
)

logger = logging.getLogger(__name__)

logger.setLevel(logging.DEBUG)


ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)

TOTAL_MESSAGES = 64
TOTAL_BATCHES = 2
BATCH_MESSAGES = TOTAL_MESSAGES // TOTAL_BATCHES
KAFKA_TRIGGER_DURATION = 16

RESPONSE_FOLDER = "response_folder"


LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


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
        ("confluent_sr_avro_serde", get_avro_message, "confluent_sr_mock_registry"),
        ("confluent_sr_json_serde", get_avro_message, "confluent_sr_mock_registry"),
        (
            "confluent_sr_protobuf_serde",
            get_protobuf_message,
            "confluent_sr_mock_registry",
        ),
        (
            "aws_glue_sr_avro_glue_serde",
            get_avro_message,
            "aws_glue_sr_mock_avro_glue_client",
        ),
        (
            "aws_glue_sr_json_glue_serde",
            get_avro_message,
            "aws_glue_sr_mock_json_glue_client",
        ),
        (
            "aws_glue_sr_protobuf_glue_serde",
            get_protobuf_message,
            "aws_glue_sr_mock_protobuf_glue_client",
        ),
    ],
)
def test_kafka_stage_trigger_time_rollover(
    serde_fixture,
    get_message,
    get_sr_client,
    tmp_path,
    request,
    kafka_config,
    produce_kafka_messages,
):
    topic = f"test-consumer-messages-rollover-{str(uuid.uuid4())}"

    serde = request.getfixturevalue(serde_fixture)
    sr_client = request.getfixturevalue(get_sr_client) if get_sr_client else None
    sr_client = sr_client[0] if isinstance(sr_client, tuple) else sr_client

    produce_kafka_messages(topic, TOTAL_MESSAGES, serde, get_message)

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
        ),
        topic=topic,
        data_format=serde.format(),
        schema=serde._schema,
        time_rollover_secs=5,
        end=datetime.now(timezone.utc) + timedelta(seconds=70),
        message_name=getattr(serde, "_message_name", None),
        registry_name=getattr(serde, "_registry_name", None),
        schema_name=getattr(serde, "_schema_name", None),
    )

    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        trigger_by=trigger,
    )
    def tfr(tfs: list[td.TableFrame]) -> td.TableFrame:

        new_tfs = td.concat(tfs)

        return new_tfs

    staging_folder = tmp_path / "staging"
    mock_context = MockLocalFileStageTriggerContext(
        staging_folder=str(staging_folder),
        trigger_config=CallConfig("POST", "http://fake.url", {}, {}),
        logger=logger,
        working_dir=str(tmp_path),
        end=trigger.end,
    )

    result = trigger.run(mock_context)
    assert result == StageTriggerStatus.SUCCESS
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        tfr, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )
    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    staging_uri = staging_folder.as_uri()
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        staging_uri=staging_uri,
        function_data_path=function_data_folder,
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
    assert tfr_result.shape[0] == TOTAL_MESSAGES - 1


@pytest.mark.kafka
@pytest.mark.slow
@pytest.mark.requires_internet
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize(
    "serde_fixture, get_message, get_sr_client",
    [
        ("avro_serde_v1", get_avro_message, None),
        # ("json_serde", get_json_message, None),
        # ("protobuf_serde_v1", get_protobuf_message, None),
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
    tmp_path,
    request,
    kafka_config,
    produce_kafka_messages,
):
    topic = str(uuid.uuid4())
    serde = request.getfixturevalue(serde_fixture)
    produce_kafka_messages(topic, TOTAL_MESSAGES, serde, get_message)

    trigger = KafkaTrigger(
        conn=KafkaConn(
            servers=kafka_config.bootstrap_servers,
            credentials=cast(UserPasswordCredentials, kafka_config.credentials),
            group_id=str(uuid.uuid4()),
        ),
        topic=topic,
        data_format=serde.format(),
        schema=serde._schema,
        messages_rollover=BATCH_MESSAGES,
        end=datetime.now(timezone.utc) + timedelta(seconds=KAFKA_TRIGGER_DURATION),
        message_name=getattr(serde, "_message_name", None),
        registry_name=getattr(serde, "_registry_name", None),
        schema_name=getattr(serde, "_schema_name", None),
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
        function=tfr,
        trigger=trigger,
    )

    staging_folder = fn_trigger.staging
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

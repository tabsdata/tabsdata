#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from google.protobuf import descriptor_pb2, message, message_factory

from tabsdata._io.stage_trigger import StageTriggerContext
from tabsdata_kafka.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
)
from tabsdata_kafka.consumer.consumer import KafkaConsumer, RolloverReason
from tabsdata_kafka.consumer.messages_buffer import MessagesBuffer
from tabsdata_kafka.typing import (
    AwsGlueSchemaRegistryCredentialsSpec,
)

logger = logging.getLogger(__name__)


def handle_rollover(
    context: StageTriggerContext,
    buffer: MessagesBuffer,
    rollover_reason: RolloverReason,
    working_dir: str,
    kafka_consumer: KafkaConsumer,
) -> float:
    """
    Handle buffer rollover: write to parquet, stage, commit, and trigger if needed.

    Args:
        context: The StageWriterContext instance.
        buffer: The MessagesBuffer to write and stage.
        rollover_reason: The reason for rollover.
        working_dir: Directory to write parquet files.
        kafka_consumer: The KafkaConsumer instance.

    Returns:
        The new start time after rollover.
    """
    logger.debug(f"Rollover reason: {rollover_reason}")
    buffer_file_path = None
    if not buffer.is_flushed:
        buffer_file_path = buffer.to_parquet(dir_path=working_dir)

    if buffer_file_path is not None:
        logger.debug(f"Buffer written to parquet file: {buffer_file_path}")
        context.stage(buffer_file_path)
        kafka_consumer.commit()
        if rollover_reason != RolloverReason.SchemaChange:
            logger.debug("Calling context.trigger() after rollover")
            context.trigger()
    else:
        logger.debug("No messages to write during rollover.")
    return time.time()


def get_protobuf_class(
    working_dir: str, protobuf_schema: str, message_name: str
) -> type[message.Message]:
    """
    Dynamically generates a Python class for a Protocol Buffers message from a given
    schema.

    Args:
        working_dir: The directory in which to create temporary files.
        protobuf_schema: The Protocol Buffers schema definition as a string.
        message_name: The name of the message type to retrieve.
    Returns:
        The generated Python class corresponding to the specified message.
    """
    with tempfile.TemporaryDirectory(dir=working_dir) as temp_dir:
        proto_file_path = Path(temp_dir) / "schema.proto"
        descriptor_set_output = Path(temp_dir) / "schema.bin"

        with open(proto_file_path, "w") as file:
            file.write(protobuf_schema)

        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{temp_dir}",
                f"-o{str(descriptor_set_output)}",
                str(proto_file_path),
            ]
        )

        with open(descriptor_set_output, "rb") as f:
            descriptor_set_data = f.read()

        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.ParseFromString(descriptor_set_data)

        message_classes = message_factory.GetMessages(file_descriptor_set.file)
        message_class = message_classes[message_name]

        return message_class


def obtain_and_set_aws_glue_credentials(
    credentials: AwsGlueSchemaRegistryCredentialsSpec,
):
    """
    Given the AwsGlueSchemaRegistryCredentialsSpec object, obtain the secret values
    and set them as required for the aws glue client to use them.

    Args:
        credentials: The AwsGlueSchemaRegistryCredentialsSpec object containing AWS Glue
            credentials.
    """
    logger.info("Setting AWS Glue credentials.")
    if isinstance(credentials, AwsGlueSchemaRegistryCredentialsSpec):
        os.environ[AWS_ACCESS_KEY_ID] = credentials.aws_access_key_id.secret_value
        os.environ[AWS_SECRET_ACCESS_KEY] = (
            credentials.aws_secret_access_key.secret_value
        )
        logger.info("AWS Glue credentials set successfully.")
    else:
        logger.error(
            f"Invalid credentials type for AWS Glue: {type(credentials)}. No "
            "data imported."
        )
        raise TypeError(
            f"Invalid credentials type for AWS Glue: {type(credentials)}. No data"
            " imported."
        )

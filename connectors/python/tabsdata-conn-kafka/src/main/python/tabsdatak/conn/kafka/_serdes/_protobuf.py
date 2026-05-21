"""Compile a Protocol Buffers schema string into a Python message
class. Used by every Protobuf serde variant (no-SR, Confluent SR,
Glue SR) to recover a ``message_class`` from the user-supplied
``.proto`` content."""

from __future__ import annotations

import logging
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover -- typing-only
    from google.protobuf import message


logger = logging.getLogger(__name__)


def get_protobuf_class(
    *,
    working_dir: str,
    protobuf_schema: str,
    message_name: str,
) -> type["message.Message"]:
    """Compile ``protobuf_schema`` via ``grpc_tools.protoc`` to a
    descriptor set, then resolve ``message_name`` against the
    generated descriptors.

    :param working_dir: Scratch dir for the temporary ``.proto``
        and descriptor-set files.
    :param protobuf_schema: ``.proto`` source.
    :param message_name: Fully-qualified message name to resolve
        (``"com.example.UserEvent"``).
    """
    from google.protobuf import descriptor_pb2, message_factory

    with tempfile.TemporaryDirectory(dir=working_dir) as temp_dir:
        proto_file_path = Path(temp_dir) / "schema.proto"
        descriptor_set_output = Path(temp_dir) / "schema.bin"

        with open(proto_file_path, "w") as fh:
            fh.write(protobuf_schema)

        try:
            subprocess.check_call([
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{temp_dir}",
                f"-o{str(descriptor_set_output)}",
                str(proto_file_path),
            ])
        except subprocess.CalledProcessError as exc:
            raise KafkaErrorCode.KAFKA_16.exception(error=exc)

        with open(descriptor_set_output, "rb") as fh:
            descriptor_set_data = fh.read()

        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.ParseFromString(descriptor_set_data)

        message_classes = message_factory.GetMessages(
            file_descriptor_set.file,
        )
        if message_name not in message_classes:
            raise KafkaErrorCode.KAFKA_17.exception(message_name=message_name)
        return message_classes[message_name]

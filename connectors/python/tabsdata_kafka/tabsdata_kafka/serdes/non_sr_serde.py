#
#  Copyright 2026. Tabs Data Inc.
#

import io
import json
from abc import ABC
from typing import Callable, Dict

import avro.io
import avro.schema
import jsonschema
from confluent_kafka.serialization import SerializationContext
from google.protobuf.json_format import MessageToDict, ParseDict

from tabsdata_kafka.constants import (
    MessageFormat,
)
from tabsdata_kafka.shared_types import (
    KafkaMessageFormatSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
)

from tabsdata_kafka.serdes.serde import (
    KafkaSerde,
)
from tabsdata_kafka.utils import get_protobuf_class


class NonSrKafkaSerde(KafkaSerde, ABC):

    def extract_schema_id(self, _data: bytes, message: Dict) -> SchemaIdSpec:
        keys = sorted(list(message.keys()))
        # noinspection PyTypeChecker
        return keys

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        return lambda a, b: set(b).issubset(set(a))


class JsonKafkaSerde(NonSrKafkaSerde):
    """
    KafkaSerde implementation for JSON messages without a schema registry,
    but with optional local JSON schema validation.
    """

    # ToDo: Simplify or reorganize to make liter exception unnecessary.
    def __init__(  # noqa: C901
        self,
        json_schema: str | None,
    ):
        super().__init__()
        if json_schema:
            try:
                json_schema = json.loads(json_schema)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON schema string provided: {e}") from e
        else:
            json_schema = None

        def serialize(obj: Dict, ctx: SerializationContext) -> bytes:
            """
            Serializes a Python dictionary to a JSON bytes string, with optional schema
            validation.
            """
            if obj is None:
                raise RuntimeError  # TODO

            if json_schema:
                try:
                    jsonschema.validate(instance=obj, schema=json_schema)
                except jsonschema.ValidationError as e:
                    raise ValueError(
                        "Serialization failed: Message does not conform to schema:"
                        f" {e.message}"
                    ) from e
                except Exception as e:
                    raise ValueError(
                        f"Serialization failed during schema validation: {e}"
                    ) from e

            # Ensure the output is a compact JSON string on a single line
            return json.dumps(obj, separators=(",", ":")).encode("utf-8")

        self._serializer = serialize

        def deserialize(bytes_data: bytes, ctx: SerializationContext) -> Dict:
            """
            Deserializes a JSON bytes string to a Python dictionary, with optional
            schema validation.
            """
            if bytes_data is None:
                raise RuntimeError  # TODO

            try:
                deserialized_data = json.loads(bytes_data.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Deserialization failed: Invalid JSON format: {e}"
                ) from e

            if json_schema:
                try:
                    jsonschema.validate(instance=deserialized_data, schema=json_schema)
                except jsonschema.ValidationError as e:
                    raise ValueError(
                        "Deserialization failed: Message does not conform to schema:"
                        f" {e.message}"
                    ) from e
                except Exception as e:
                    raise ValueError(
                        f"Deserialization failed during schema validation: {e}"
                    ) from e

            return deserialized_data

        self._deserializer = deserialize
        self._schema = json.dumps(json_schema)

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.JSON.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        """
        Returns a serializer function that takes an object and a SerializationContext
        and returns bytes.
        """
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        """
        Returns a deserializer function that takes bytes and a SerializationContext
        and returns an object.
        """
        return self._deserializer


class AvroKafkaSerde(NonSrKafkaSerde):
    """
    Concrete implementation of KafkaSerde for Avro schemas, without using Schema
    Registry.
    Serialization and deserialization are based on a provided Avro schema file.
    """

    def __init__(self, avro_schema: str):
        """
        Initializes the Avro serde with an Avro schema.
        """
        super().__init__()
        schema = avro.schema.parse(avro_schema)

        def serializer(obj: Dict, _ctx: SerializationContext) -> bytes:
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(obj, encoder)
            return bytes_writer.getvalue()

        self._serializer = serializer

        def deserializer(data: bytes, _ctx: SerializationContext) -> Dict:
            bytes_reader = io.BytesIO(data)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            # noinspection PyTypeChecker
            return reader.read(decoder)

        self._deserializer = deserializer
        self._schema = avro_schema

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.AVRO.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer


class ProtobufKafkaSerde(NonSrKafkaSerde):
    """
    Concrete implementation of KafkaSerde for Protobuf schemas, without using Schema
    Registry.
    It dynamically generates a Python module from a .proto file and imports it.
    The serializer converts a dictionary to a Protobuf message before serializing.
    The deserializer parses the Protobuf message and converts it to a dictionary.
    """

    def __init__(self, protobuf_schema: str, message_name: str, working_dir: str):
        """
        Initializes the Protobuf serde.
        :param protobuf_schema: The protobuf schema.
        :param message_name: The name of the message class to be used for
                             serialization/deserialization (e.g., 'MyMessage').
        :param working_dir: A working directory used for runtime files by the serde.
        """
        super().__init__()
        msg_class = get_protobuf_class(working_dir, protobuf_schema, message_name)

        def serializer(obj_dict: Dict, _ctx: SerializationContext) -> bytes:
            message = msg_class()
            ParseDict(obj_dict, message)
            return message.SerializeToString()

        self._serializer = serializer

        def deserializer(data: bytes, _ctx: SerializationContext) -> Dict:
            message = msg_class()
            message.ParseFromString(data)
            return MessageToDict(message, preserving_proto_field_name=True)

        self._deserializer = deserializer
        self._schema = protobuf_schema
        self._message_name = message_name
        self._working_dir = working_dir

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.PROTOBUF.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer

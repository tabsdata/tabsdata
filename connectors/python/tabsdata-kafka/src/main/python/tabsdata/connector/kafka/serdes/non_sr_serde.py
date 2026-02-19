#
#  Copyright 2026. Tabs Data Inc.
#

from __future__ import annotations

import io
import json
from abc import ABC
from typing import TYPE_CHECKING, Callable, cast

import avro.io
import avro.schema
import jsonschema
from google.protobuf.json_format import MessageToDict, ParseDict

if TYPE_CHECKING:
    from confluent_kafka.serialization import SerializationContext

from tabsdata.connector.kafka.constants import (
    _AVRO,
    _JSON,
    _PROTOBUF,
)
from tabsdata.connector.kafka.serdes.serde import (
    KafkaSerde,
)
from tabsdata.connector.kafka.typing import (
    SchemaIdCompatibles,
    SchemaIdSpec,
    _KafkaMessageFormatNameSpec,
)
from tabsdata.connector.kafka.utils import get_protobuf_class


class NonSrKafkaSerde(KafkaSerde, ABC):
    def extract_schema_id(self, _data: bytes, message: dict) -> SchemaIdSpec:
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

    def __init__(self, json_schema: str | None):
        """
        Initializes the JSON serde with an optional JSON schema.

        Args:
            json_schema: Optional JSON schema string.
        """
        super().__init__()
        json_schema = self._parse_json_schema(json_schema)
        self._serializer = self._make_serializer(json_schema)
        self._deserializer = self._make_deserializer(json_schema)
        self._schema = json.dumps(json_schema) if json_schema else None

    @staticmethod
    def _parse_json_schema(json_schema: str | None):
        if json_schema:
            try:
                return json.loads(json_schema)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON schema string provided: {e}") from e
        return None

    @staticmethod
    def _make_serializer(json_schema):
        def serialize(obj: dict, _: SerializationContext) -> bytes:
            if obj is None:
                raise RuntimeError("Serialization failed: 'obj' is None")

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

            return json.dumps(obj, separators=(",", ":")).encode("utf-8")

        return serialize

    @staticmethod
    def _make_deserializer(json_schema):
        def deserialize(bytes_data: bytes, _: SerializationContext) -> dict:
            if bytes_data is None:
                raise RuntimeError("Deserialization failed: 'bytes_data' is None")

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

        return deserialize

    @property
    def format(self) -> _KafkaMessageFormatNameSpec:
        return cast(_KafkaMessageFormatNameSpec, _JSON)

    def serializer(self) -> Callable[[dict, SerializationContext], bytes]:
        """
        Returns a serializer function that takes an object and a SerializationContext
        and returns bytes.
        """
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], dict]:
        """
        Returns a deserializer function that takes bytes and a SerializationContext
        and returns an object.
        """
        return self._deserializer

    def __repr__(self):
        return f"JsonKafkaSerde(schema={self._schema})"


class AvroKafkaSerde(NonSrKafkaSerde):
    """
    Concrete implementation of KafkaSerde for Avro schemas, without using Schema
    Registry.
    Serialization and deserialization are based on a provided Avro schema file.
    """

    def __init__(self, avro_schema: str):
        """
        Initializes the Avro serde with an Avro schema.

        Args:
            avro_schema: The Avro schema as a string.
        """
        super().__init__()
        schema = avro.schema.parse(avro_schema)
        self._serializer = self._make_serializer(schema)
        self._deserializer = self._make_deserializer(schema)
        self._schema = avro_schema

    @staticmethod
    def _make_serializer(schema):
        def serializer(obj: dict, _ctx: SerializationContext) -> bytes:
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(obj, encoder)
            return bytes_writer.getvalue()

        return serializer

    @staticmethod
    def _make_deserializer(schema):
        def deserializer(data: bytes, _ctx: SerializationContext) -> dict:
            bytes_reader = io.BytesIO(data)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(schema)
            # noinspection PyTypeChecker
            return reader.read(decoder)

        return deserializer

    @property
    def format(self) -> _KafkaMessageFormatNameSpec:
        return cast(_KafkaMessageFormatNameSpec, _AVRO)

    def serializer(self) -> Callable[[dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], dict]:
        return self._deserializer

    def __repr__(self):
        return f"AvroKafkaSerde(schema={self._schema})"


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

        Args:
            protobuf_schema: The protobuf schema.
            message_name: The name of the message class to be used for
                          serialization/deserialization.
            working_dir: A working directory used for runtime files by the serde.
        """
        super().__init__()
        msg_class = get_protobuf_class(working_dir, protobuf_schema, message_name)

        self._serializer = self._make_serializer(msg_class)
        self._deserializer = self._make_deserializer(msg_class)
        self._schema = protobuf_schema
        self._message_name = message_name
        self._working_dir = working_dir

    @staticmethod
    def _make_serializer(msg_class):
        def serializer(obj_dict: dict, _ctx: SerializationContext) -> bytes:
            message = msg_class()
            ParseDict(obj_dict, message)
            return message.SerializeToString()

        return serializer

    @staticmethod
    def _make_deserializer(msg_class):
        def deserializer(data: bytes, _ctx: SerializationContext) -> dict:
            message = msg_class()
            message.ParseFromString(data)
            return MessageToDict(message, preserving_proto_field_name=True)

        return deserializer

    @property
    def format(self) -> _KafkaMessageFormatNameSpec:
        return cast(_KafkaMessageFormatNameSpec, _PROTOBUF)

    def serializer(self) -> Callable[[dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], dict]:
        return self._deserializer

    def __repr__(self):
        return (
            f"ProtobufKafkaSerde(schema={self._schema},"
            f" message_name={self._message_name}, working_dir={self._working_dir})"
        )

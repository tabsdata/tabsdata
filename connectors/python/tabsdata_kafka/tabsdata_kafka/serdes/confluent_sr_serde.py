#
# Copyright 2026 Tabs Data Inc.
#

from abc import ABC
from typing import Callable, Dict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
)
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


class SrKafkaSerde(KafkaSerde, ABC):

    def extract_schema_id(self, data: bytes, _message: Dict) -> SchemaIdSpec:
        if len(data) < 5:
            raise ValueError(
                "Payload is too short to be a Confluent Schema Registry message"
            )

        schema_id_bytes = data[1:5]
        return str(int.from_bytes(schema_id_bytes, byteorder="big"))

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        return lambda a, b: a == b


class AvroSrKafkaSerde(SrKafkaSerde):
    """
    KafkaSerde implementation for Avro schemas with Schema Registry.
    """

    def __init__(
        self, schema_registry_client: SchemaRegistryClient, avro_schema: str = None
    ):
        super().__init__()
        self._serializer = AvroSerializer(
            schema_str=avro_schema,
            schema_registry_client=schema_registry_client,
            to_dict=lambda obj, ctx: obj,
        )
        self._deserializer = AvroDeserializer(
            schema_str=avro_schema,
            schema_registry_client=schema_registry_client,
            from_dict=lambda obj, ctx: obj,
        )
        self._schema = avro_schema

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.AVRO.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer


class JsonSrKafkaSerde(SrKafkaSerde):
    """
    KafkaSerde implementation for Json schemas.
    """

    def __init__(
        self, schema_registry_client: SchemaRegistryClient, json_schema: str = None
    ):
        super().__init__()
        self._serializer = JSONSerializer(
            schema_str=json_schema, schema_registry_client=schema_registry_client
        )
        self._deserializer = JSONDeserializer(
            schema_str=json_schema, schema_registry_client=schema_registry_client
        )
        self._schema = json_schema

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.JSON.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer


class ProtobufSrKafkaSerde(SrKafkaSerde):
    """
    KafkaSerde implementation for Protobuf schemas with Schema Registry.
    """

    def __init__(
        self,
        schema_registry_client: SchemaRegistryClient,
        protobuf_schema: str,
        message_name: str,
        working_dir: str,
    ):
        """
        Initializes the Protobuf serde.
        :param protobuf_schema: The protobuf schema.
        :param message_name: The name of the message class to be used for
                             serialization/deserialization (e.g., 'MyMessage').
        :param working_dir: A working directory used for runtime files by the serde.
        """
        super().__init__()
        msg_class = get_protobuf_class(working_dir, protobuf_schema, message_name)

        class_serializer = ProtobufSerializer(
            msg_type=msg_class,
            schema_registry_client=schema_registry_client,
        )

        def serializer(dict_obj: Dict, ctx: SerializationContext) -> bytes:
            obj = msg_class()
            ParseDict(dict_obj, obj)
            return class_serializer(obj, ctx)

        self._serializer = serializer

        class_deserializer = ProtobufDeserializer(
            message_type=msg_class,
            schema_registry_client=schema_registry_client,
        )

        def deserializer(data: bytes, ctx: SerializationContext) -> Dict:
            obj = class_deserializer(data, ctx)
            return MessageToDict(
                obj,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=False,
            )

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

#
#  Copyright 2026. Tabs Data Inc.
#

import json
import uuid
import zlib
from abc import ABC
from functools import lru_cache
from io import BytesIO
from typing import Callable, Dict

import jsonschema
from confluent_kafka.serialization import Deserializer, SerializationContext, Serializer
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from tabsdata_kafka.constants import (
    MessageFormat,
)
from tabsdata_kafka.serdes.serde import (
    KafkaSerde,
)
from tabsdata_kafka.shared_types import (
    KafkaMessageFormatSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
)
from tabsdata_kafka.utils import get_protobuf_class


class GlueKafkaSerde(KafkaSerde, ABC):

    def extract_schema_id(self, data: bytes, _message: Dict) -> SchemaIdSpec:
        if len(data) < 18:
            raise ValueError(
                "Payload is too short to be an AWS Glue Schema Registry message"
            )

        schema_uuid_bytes = data[2:18]
        return str(uuid.UUID(bytes=schema_uuid_bytes))

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        return lambda a, b: a == b


class AwsGlueSerdeBase:
    """
    Base class for AWS Glue Schema Registry serde.
    """

    HEADER_VERSION_BYTE = 3
    COMPRESSION_DEFAULT = 0
    COMPRESSION_ZLIB = 5

    def __init__(self, glue_client, **configs):
        self.glue_client = glue_client


class AwsGlueSerializer(AwsGlueSerdeBase, Serializer):
    """
    Base class for AWS Glue Schema Registry serializers.
    Handles the wire format specification.
    """

    def __init__(
        self,
        glue_client,
        registry_name: str,
        schema_name: str,
        schema_definition: str,
        data_format: str,
    ):
        super().__init__(glue_client=glue_client)
        self.registry_name = registry_name
        self.schema_name = schema_name
        self.schema_definition = schema_definition
        self.data_format = data_format
        self._schema_version_id = self._get_schema_version_id()

    @lru_cache(maxsize=100)
    def _get_schema_version_id(self) -> str:
        try:
            response = self.glue_client.get_schema_by_definition(
                SchemaId={
                    "RegistryName": self.registry_name,
                    "SchemaName": self.schema_name,
                },
                SchemaDefinition=self.schema_definition,
            )
            return response["SchemaVersionId"]
        except self.glue_client.exceptions.EntityNotFoundException:
            try:
                response = self.glue_client.register_schema_version(
                    SchemaId={
                        "RegistryName": self.registry_name,
                        "SchemaName": self.schema_name,
                    },
                    SchemaDefinition=self.schema_definition,
                )
                return response["SchemaVersionId"]
            except self.glue_client.exceptions.EntityNotFoundException:
                response = self.glue_client.create_schema(
                    RegistryId={"RegistryName": self.registry_name},
                    SchemaName=self.schema_name,
                    DataFormat=self.data_format.upper(),
                    Compatibility="NONE",
                    SchemaDefinition=self.schema_definition,
                )
                return response["SchemaVersionId"]

    def pack_data(self, data: bytes) -> bytes:
        """
        Packs the data with the AWS Glue wire format.
        [Header(1)] + [Compression(1)] + [UUID(16)] + [Data(N)]
        """
        schema_version_id_bytes = uuid.UUID(self._schema_version_id).bytes
        compression_byte = self.COMPRESSION_DEFAULT
        return (
            bytes([self.HEADER_VERSION_BYTE, compression_byte])
            + schema_version_id_bytes
            + data
        )


class AwsGlueDeserializer(AwsGlueSerdeBase, Deserializer):
    """
    Base class that handles AWS Glue Wire Format:
    [Header(1)] + [Compression(1)] + [UUID(16)] + [Data(N)]
    """

    def __init__(self, glue_client, **configs):
        super().__init__(glue_client=glue_client, **configs)

    @lru_cache(maxsize=1000)
    def get_schema_definition(self, schema_uuid_str):
        """Fetches schema definition from AWS with LRU Caching to save API calls"""
        response = self.glue_client.get_schema_version(SchemaVersionId=schema_uuid_str)
        return response["SchemaDefinition"]

    def unpack_data(self, payload):
        if len(payload) < 18:
            raise ValueError(
                "Payload is too short to be an AWS Glue Schema Registry message"
            )

        header_ver = payload[0]
        compression_byte = payload[1]

        if header_ver != self.HEADER_VERSION_BYTE:
            raise ValueError(f"Unsupported header version: {header_ver}")

        schema_uuid_bytes = payload[2:18]
        schema_uuid = str(uuid.UUID(bytes=schema_uuid_bytes))

        data_bytes = payload[18:]

        if compression_byte == self.COMPRESSION_ZLIB:
            data_bytes = zlib.decompress(data_bytes)

        return schema_uuid, data_bytes


class JsonGlueSerializer(AwsGlueSerializer):
    def __init__(
        self, glue_client, registry_name: str, schema_name: str, schema_definition: str
    ):
        super().__init__(
            glue_client, registry_name, schema_name, schema_definition, "JSON"
        )

    def __call__(self, obj: Dict, ctx: SerializationContext = None) -> bytes:
        if obj is None:
            return None
        json_bytes = json.dumps(obj).encode("utf-8")
        return self.pack_data(json_bytes)


class JsonGlueDeserializer(AwsGlueDeserializer):
    def __init__(self, glue_client, validate_schema: bool = True):
        super().__init__(glue_client=glue_client)
        self.validate = validate_schema

    def __call__(self, data: bytes, ctx: SerializationContext = None) -> Dict:
        if data is None:
            return None

        schema_uuid, json_bytes = self.unpack_data(data)
        obj = json.loads(json_bytes)

        if self.validate:
            schema_str = self.get_schema_definition(schema_uuid)
            schema = json.loads(schema_str)
            jsonschema.validate(instance=obj, schema=schema)

        return obj


class JsonGlueKafkaSerde(GlueKafkaSerde):
    def __init__(
        self,
        glue_client,
        registry_name: str,
        schema_name: str,
        json_schema: str,
        validate_schema: bool = True,
    ):
        super().__init__()
        self._serializer = JsonGlueSerializer(
            glue_client, registry_name, schema_name, json_schema
        )
        self._deserializer = JsonGlueDeserializer(glue_client, validate_schema)
        self._schema = json_schema
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._glue_client = glue_client

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.JSON.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer


class AvroGlueSerializer(AwsGlueSerializer):
    def __init__(
        self, glue_client, registry_name: str, schema_name: str, schema_definition: str
    ):
        super().__init__(
            glue_client, registry_name, schema_name, schema_definition, "AVRO"
        )
        self.parsed_schema = parse_schema(json.loads(schema_definition))

    def __call__(self, obj: Dict, ctx: SerializationContext = None) -> bytes:
        if obj is None:
            return None

        with BytesIO() as bio:
            schemaless_writer(bio, self.parsed_schema, obj)
            avro_bytes = bio.getvalue()

        return self.pack_data(avro_bytes)


class AvroGlueDeserializer(AwsGlueDeserializer):
    def __init__(self, glue_client):
        super().__init__(glue_client=glue_client)
        self._parsed_schema_cache = {}

    def _get_parsed_schema(self, schema_uuid):
        if schema_uuid not in self._parsed_schema_cache:
            schema_str = self.get_schema_definition(schema_uuid)
            self._parsed_schema_cache[schema_uuid] = parse_schema(
                json.loads(schema_str)
            )
        return self._parsed_schema_cache[schema_uuid]

    def __call__(self, data: bytes, ctx: SerializationContext = None) -> Dict:
        if data is None:
            return None

        schema_uuid, avro_bytes = self.unpack_data(data)
        writer_schema = self._get_parsed_schema(schema_uuid)

        with BytesIO(avro_bytes) as bio:
            return schemaless_reader(bio, writer_schema)


class AvroGlueKafkaSerde(GlueKafkaSerde):
    def __init__(
        self, glue_client, registry_name: str, schema_name: str, avro_schema: str
    ):
        super().__init__()
        self._serializer = AvroGlueSerializer(
            glue_client, registry_name, schema_name, avro_schema
        )
        self._deserializer = AvroGlueDeserializer(glue_client)
        self._schema = avro_schema
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._glue_client = glue_client

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.AVRO.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer


class ProtobufGlueSerializer(AwsGlueSerializer):
    def __init__(
        self,
        glue_client,
        registry_name: str,
        schema_name: str,
        schema_definition: str,
        msg_class: Message,
    ):
        super().__init__(
            glue_client, registry_name, schema_name, schema_definition, "PROTOBUF"
        )
        self.msg_class = msg_class

    def __call__(self, obj: Message, ctx: SerializationContext = None) -> bytes:
        if obj is None:
            return None

        proto_bytes = obj.SerializeToString()
        return self.pack_data(proto_bytes)


class ProtobufGlueDeserializer(AwsGlueDeserializer):
    def __init__(self, glue_client, msg_class: Message):
        super().__init__(glue_client=glue_client)
        self.msg_class = msg_class

    def __call__(self, data: bytes, ctx: SerializationContext = None) -> Dict:
        if data is None:
            return None

        _, proto_bytes = self.unpack_data(data)

        obj = self.msg_class()
        obj.ParseFromString(proto_bytes)
        return obj


class ProtobufGlueKafkaSerde(GlueKafkaSerde):
    def __init__(
        self,
        glue_client,
        registry_name: str,
        schema_name: str,
        protobuf_schema: str,
        message_name: str,
        working_dir: str,
    ):
        super().__init__()
        msg_class = get_protobuf_class(working_dir, protobuf_schema, message_name)

        class_serializer = ProtobufGlueSerializer(
            glue_client, registry_name, schema_name, protobuf_schema, msg_class
        )

        def serializer(dict_obj: Dict, ctx: SerializationContext) -> bytes:
            obj = msg_class()
            ParseDict(dict_obj, obj)
            return class_serializer(obj, ctx)

        self._serializer = serializer

        class_deserializer = ProtobufGlueDeserializer(glue_client, msg_class)

        def deserializer(data: bytes, ctx: SerializationContext) -> Dict:
            obj = class_deserializer(data, ctx)
            return MessageToDict(
                obj,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=False,
            )

        self._deserializer = deserializer
        self._schema = protobuf_schema
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._message_name = message_name
        self._working_dir = working_dir
        self._glue_client = glue_client

    def format(self) -> KafkaMessageFormatSpec:
        return MessageFormat.PROTOBUF.value

    def serializer(self) -> Callable[[Dict, SerializationContext], bytes]:
        return self._serializer

    def deserializer(self) -> Callable[[bytes, SerializationContext], Dict]:
        return self._deserializer

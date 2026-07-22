#
# Copyright 2026 Tabsdata Inc.
#

"""AWS Glue Schema Registry serdes.

AWS Glue's wire format prepends an 18-byte header to every
payload: 1 magic byte (``0x03``) + 1 compression byte + 16
schema-version UUID bytes. The serdes extract that UUID and the
``MessagesBuffer`` keys on it (exact-UUID equality).

Schema-version resolution goes through the boto3 Glue client
(``get_schema_by_definition`` -> ``register_schema_version`` ->
``create_schema`` fallback chain on the serialiser side;
``get_schema_version`` on the deserialiser side, with an LRU
cache so we don't roundtrip per message).
"""

from __future__ import annotations

import json
import logging
import uuid
import zlib
from abc import ABC
from functools import lru_cache
from io import BytesIO
from typing import TYPE_CHECKING, Any, Callable, cast

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._base import (
    KafkaSerde,
    MsgFormatSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._protobuf import get_protobuf_class
from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka.serialization import (
        SerializationContext,
    )


logger = logging.getLogger(__name__)


# AWS Glue wire-format constants.
_HEADER_VERSION_BYTE = 3
_COMPRESSION_DEFAULT = 0
_COMPRESSION_ZLIB = 5


class GlueKafkaSerde(KafkaSerde, ABC):
    """Common base for the three Glue SR serdes. The wire header
    is ``[ver(1)][comp(1)][uuid(16)]`` -- bytes 2..18 are the
    schema-version UUID, which becomes the buffer's schema id."""

    def extract_schema_id(
        self,
        data: bytes,
        _message: dict,
    ) -> SchemaIdSpec:
        if data is None or len(data) < 18:
            raise KafkaErrorCode.KAFKA_13.exception(
                length=0 if data is None else len(data),
            )
        return str(uuid.UUID(bytes=data[2:18]))

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        return lambda a, b: a == b


# ---------------------------------------------------------------------
# Wire-format helpers (mirrors the legacy ``AwsGlueSerdeBase``)
# ---------------------------------------------------------------------


class _AwsGlueSerializer:
    """Wire-format serialiser base. Resolves the schema-version
    id once on construction; ``pack_data`` prepends the 18-byte
    header to the format-specific payload bytes."""

    def __init__(
        self,
        *,
        glue_client: Any,
        registry_name: str,
        schema_name: str,
        schema_definition: str,
        data_format: str,
    ):
        self.glue_client = glue_client
        self.registry_name = registry_name
        self.schema_name = schema_name
        self.schema_definition = schema_definition
        self.data_format = data_format
        self._schema_version_id = self._resolve_schema_version_id()

    @lru_cache(maxsize=100)
    def _resolve_schema_version_id(self) -> str:
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
        return (
            bytes([_HEADER_VERSION_BYTE, _COMPRESSION_DEFAULT])
            + uuid.UUID(self._schema_version_id).bytes
            + data
        )


class _AwsGlueDeserializer:
    """Wire-format deserialiser base. Unpacks the header, resolves
    the schema-version id via boto3 (with LRU cache), and returns
    the inner payload bytes for the format-specific decoder."""

    def __init__(self, *, glue_client: Any):
        self.glue_client = glue_client

    @lru_cache(maxsize=1000)
    def get_schema_definition(self, schema_uuid_str: str) -> str:
        try:
            response = self.glue_client.get_schema_version(
                SchemaVersionId=schema_uuid_str,
            )
            return response["SchemaDefinition"]
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_18.exception(
                cause=exc,
                registry_name="(deserialise)",
                schema_name=schema_uuid_str,
            )

    @staticmethod
    def unpack_data(payload: bytes) -> tuple[str, bytes]:
        if len(payload) < 18:
            raise KafkaErrorCode.KAFKA_13.exception(length=len(payload))
        header_ver = payload[0]
        compression_byte = payload[1]
        if header_ver != _HEADER_VERSION_BYTE:
            raise ValueError(
                f"Unsupported Glue header version: {header_ver}",
            )
        schema_uuid = str(uuid.UUID(bytes=payload[2:18]))
        data_bytes = payload[18:]
        if compression_byte == _COMPRESSION_ZLIB:
            data_bytes = zlib.decompress(data_bytes)
        return schema_uuid, data_bytes


# ---------------------------------------------------------------------
# JSON over Glue
# ---------------------------------------------------------------------


class JsonGlueKafkaSerde(GlueKafkaSerde):
    """Glue SR + JSON Schema."""

    def __init__(
        self,
        *,
        glue_client: Any,
        registry_name: str,
        schema_name: str,
        json_schema: str,
        validate_schema: bool = True,
    ):
        super().__init__()
        self._inner_serialiser = _AwsGlueSerializer(
            glue_client=glue_client,
            registry_name=registry_name,
            schema_name=schema_name,
            schema_definition=json_schema,
            data_format="json",
        )
        self._inner_deserialiser = _AwsGlueDeserializer(
            glue_client=glue_client,
        )
        self._validate = validate_schema
        self._schema = json_schema
        self._registry_name = registry_name
        self._schema_name = schema_name

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        inner = self._inner_serialiser

        def _serialise(obj: dict, _ctx) -> bytes:
            if obj is None:
                raise ValueError("Serialisation failed: 'obj' is None")
            return inner.pack_data(
                json.dumps(obj, separators=(",", ":")).encode("utf-8"),
            )

        return _serialise

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        inner = self._inner_deserialiser
        validate = self._validate

        def _deserialise(data: bytes, _ctx) -> dict:
            if data is None:
                raise ValueError("Deserialisation failed: payload is None")
            schema_uuid, json_bytes = inner.unpack_data(data)
            obj = json.loads(json_bytes)
            if validate:
                import jsonschema

                schema_str = inner.get_schema_definition(schema_uuid)
                jsonschema.validate(
                    instance=obj,
                    schema=json.loads(schema_str),
                )
            return obj

        return _deserialise

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "json")


# ---------------------------------------------------------------------
# Avro over Glue
# ---------------------------------------------------------------------


class AvroGlueKafkaSerde(GlueKafkaSerde):
    """Glue SR + Avro. Uses fastavro for the inner Avro
    encode/decode."""

    def __init__(
        self,
        *,
        glue_client: Any,
        registry_name: str,
        schema_name: str,
        avro_schema: str,
    ):
        super().__init__()
        from fastavro import parse_schema

        self._inner_serialiser = _AwsGlueSerializer(
            glue_client=glue_client,
            registry_name=registry_name,
            schema_name=schema_name,
            schema_definition=avro_schema,
            data_format="avro",
        )
        self._inner_deserialiser = _AwsGlueDeserializer(
            glue_client=glue_client,
        )
        self._parsed_schema = parse_schema(json.loads(avro_schema))
        self._parsed_schema_cache: dict[str, Any] = {}
        self._schema = avro_schema
        self._registry_name = registry_name
        self._schema_name = schema_name

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        from fastavro import schemaless_writer

        inner = self._inner_serialiser
        parsed = self._parsed_schema

        def _serialise(obj: dict, _ctx) -> bytes:
            if obj is None:
                raise ValueError("Serialisation failed: 'obj' is None")
            with BytesIO() as buf:
                schemaless_writer(buf, parsed, obj)
                avro_bytes = buf.getvalue()
            return inner.pack_data(avro_bytes)

        return _serialise

    def _get_parsed_schema(self, schema_uuid: str):
        if schema_uuid not in self._parsed_schema_cache:
            from fastavro import parse_schema

            schema_str = self._inner_deserialiser.get_schema_definition(
                schema_uuid,
            )
            self._parsed_schema_cache[schema_uuid] = parse_schema(
                json.loads(schema_str),
            )
        return self._parsed_schema_cache[schema_uuid]

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        from fastavro import schemaless_reader

        inner = self._inner_deserialiser

        def _deserialise(data: bytes, _ctx) -> dict:
            if data is None:
                raise ValueError("Deserialisation failed: payload is None")
            schema_uuid, avro_bytes = inner.unpack_data(data)
            writer_schema = self._get_parsed_schema(schema_uuid)
            with BytesIO(avro_bytes) as buf:
                return schemaless_reader(buf, writer_schema)

        return _deserialise

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "avro")


# ---------------------------------------------------------------------
# Protobuf over Glue
# ---------------------------------------------------------------------


class ProtobufGlueKafkaSerde(GlueKafkaSerde):
    """Glue SR + Protobuf."""

    def __init__(
        self,
        *,
        glue_client: Any,
        registry_name: str,
        schema_name: str,
        protobuf_schema: str,
        message_name: str,
        working_dir: str,
    ):
        super().__init__()
        msg_class = get_protobuf_class(
            working_dir=working_dir,
            protobuf_schema=protobuf_schema,
            message_name=message_name,
        )
        self._inner_serialiser = _AwsGlueSerializer(
            glue_client=glue_client,
            registry_name=registry_name,
            schema_name=schema_name,
            schema_definition=protobuf_schema,
            data_format="protobuf",
        )
        self._inner_deserialiser = _AwsGlueDeserializer(
            glue_client=glue_client,
        )
        self._msg_class = msg_class
        self._schema = protobuf_schema
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._message_name = message_name

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        from google.protobuf.json_format import ParseDict

        inner = self._inner_serialiser
        msg_class = self._msg_class

        def _serialise(dict_obj: dict, _ctx) -> bytes:
            if dict_obj is None:
                raise ValueError("Serialisation failed: 'dict_obj' is None")
            obj = msg_class()
            ParseDict(dict_obj, obj)
            return inner.pack_data(obj.SerializeToString())

        return _serialise

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        from google.protobuf.json_format import MessageToDict

        inner = self._inner_deserialiser
        msg_class = self._msg_class

        def _deserialise(data: bytes, _ctx) -> dict:
            if data is None:
                raise ValueError("Deserialisation failed: payload is None")
            _uuid_str, proto_bytes = inner.unpack_data(data)
            obj = msg_class()
            obj.ParseFromString(proto_bytes)
            return MessageToDict(obj, preserving_proto_field_name=True)

        return _deserialise

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "protobuf")

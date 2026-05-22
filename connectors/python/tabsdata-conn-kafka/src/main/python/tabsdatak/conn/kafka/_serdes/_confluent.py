"""Confluent-Schema-Registry serdes.

Confluent's wire format prepends a 5-byte header to every payload:
1 magic byte (``0x00``) + 4 bytes of big-endian schema id. The
serdes extract that id directly from the bytes; the
``MessagesBuffer`` keys on it (exact-id equality is the
compatibility rule)."""

from __future__ import annotations

from abc import ABC
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
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.serialization import SerializationContext


class SrKafkaSerde(KafkaSerde, ABC):
    """Common base for Confluent SR serdes. The first byte of the
    payload is a magic ``0x00``; bytes 1..5 carry the schema id as
    a big-endian unsigned int."""

    def extract_schema_id(
        self,
        data: bytes,
        _message: dict,
    ) -> SchemaIdSpec:
        if data is None or len(data) < 5:
            raise KafkaErrorCode.KAFKA_13.exception(
                length=0 if data is None else len(data),
            )
        return str(int.from_bytes(data[1:5], byteorder="big"))

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        # Exact schema-id equality; SR-aware buffers only merge
        # messages with the same registered id.
        return lambda a, b: a == b


class AvroSrKafkaSerde(SrKafkaSerde):
    """Confluent SR + Avro. Uses the Confluent Avro
    serializer/deserializer underneath."""

    def __init__(
        self,
        *,
        schema_registry_client: "SchemaRegistryClient",
        avro_schema: str | None = None,
    ):
        super().__init__()
        from confluent_kafka.schema_registry.avro import (
            AvroDeserializer,
            AvroSerializer,
        )

        self._serializer = AvroSerializer(  # type: ignore[call-arg]
            schema_str=avro_schema,
            schema_registry_client=schema_registry_client,
            to_dict=lambda obj, _ctx: obj,
        )
        self._deserializer = AvroDeserializer(  # type: ignore[call-arg]
            schema_str=avro_schema,
            schema_registry_client=schema_registry_client,
            from_dict=lambda obj, _ctx: obj,
        )
        self._schema = avro_schema

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "avro")

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        return self._serializer

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        return self._deserializer


class JsonSrKafkaSerde(SrKafkaSerde):
    """Confluent SR + JSON Schema."""

    def __init__(
        self,
        *,
        schema_registry_client: "SchemaRegistryClient",
        json_schema: str | None = None,
    ):
        super().__init__()
        from confluent_kafka.schema_registry.json_schema import (
            JSONDeserializer,
            JSONSerializer,
        )

        self._serializer = JSONSerializer(  # type: ignore[call-arg]
            schema_str=json_schema,
            schema_registry_client=schema_registry_client,
        )
        self._deserializer = JSONDeserializer(  # type: ignore[call-arg]
            schema_str=json_schema,
            schema_registry_client=schema_registry_client,
        )
        self._schema = json_schema

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "json")

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        return self._serializer

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        return cast(
            Callable[[bytes, "SerializationContext"], dict],
            self._deserializer,
        )


class ProtobufSrKafkaSerde(SrKafkaSerde):
    """Confluent SR + Protobuf.

    Needs *separate* SR clients for serialiser and deserialiser:
    the Confluent SR client caches schemas by id; the serializer's
    ``register`` call caches the schema in proto-text format
    (Redpanda v26+ returns that on read), but the deserializer
    needs the binary-format schema (``format=serialized``). A
    shared client causes a cache hit with the wrong format and
    breaks protobuf deserialisation."""

    def __init__(
        self,
        *,
        serializer_registry_client: "SchemaRegistryClient",
        deserializer_registry_client: "SchemaRegistryClient",
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
        self._serializer = self._make_serializer(
            serializer_registry_client,
            msg_class,
        )
        self._deserializer = self._make_deserializer(
            deserializer_registry_client,
            msg_class,
        )
        self._schema = protobuf_schema
        self._message_name = message_name
        self._working_dir = working_dir

    @staticmethod
    def _make_serializer(schema_registry_client: Any, msg_class: type):
        from confluent_kafka.schema_registry.protobuf import (
            ProtobufSerializer,
        )
        from google.protobuf.json_format import ParseDict

        class_serializer = ProtobufSerializer(  # type: ignore[call-arg]
            msg_type=msg_class,
            schema_registry_client=schema_registry_client,
        )

        def serializer(dict_obj: dict, ctx: "SerializationContext") -> bytes:
            obj = msg_class()
            ParseDict(dict_obj, obj)
            return class_serializer(obj, ctx)

        return serializer

    @staticmethod
    def _make_deserializer(schema_registry_client: Any, msg_class: type):
        from confluent_kafka.schema_registry.protobuf import (
            ProtobufDeserializer,
        )
        from google.protobuf.json_format import MessageToDict

        class_deserializer = ProtobufDeserializer(  # type: ignore[call-arg]
            message_type=msg_class,
            schema_registry_client=schema_registry_client,
        )

        def deserializer(data: bytes, ctx: "SerializationContext") -> dict:
            obj = class_deserializer(data, ctx)
            return MessageToDict(obj, preserving_proto_field_name=True)

        return deserializer

    @property
    def format(self) -> MsgFormatSpec:
        return cast(MsgFormatSpec, "protobuf")

    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]:
        return self._serializer

    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]:
        return self._deserializer

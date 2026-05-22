"""No-schema-registry serdes.

The schema lives client-side (the user supplies it as a string
on the ``KafkaStreamSrc``). The message-buffer compatibility key
is the sorted tuple of top-level field names; a new message is
compatible with the buffer's current message if the new message's
field set is a *subset* of the buffer's (legacy semantics).
"""

from __future__ import annotations

import io
import json
import logging
from abc import ABC
from typing import TYPE_CHECKING, Callable, cast

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
    from confluent_kafka.serialization import SerializationContext


logger = logging.getLogger(__name__)


class NonSrKafkaSerde(KafkaSerde, ABC):
    """Common base for the three no-SR serdes. Reports the
    message's field-name tuple as the schema id, and accepts new
    messages whose key set is a subset of the current buffer's."""

    def extract_schema_id(
        self,
        _data: bytes,
        message: dict,
    ) -> SchemaIdSpec:
        return tuple(sorted(message.keys()))

    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        return lambda a, b: set(b).issubset(set(a))


class JsonKafkaSerde(NonSrKafkaSerde):
    """No-SR JSON. Optional client-side schema; when supplied,
    serialise + deserialise round-trip through ``jsonschema``
    validation."""

    def __init__(self, json_schema: str | None = None):
        super().__init__()
        parsed = self._parse_json_schema(json_schema)
        self._serializer = self._make_serializer(parsed)
        self._deserializer = self._make_deserializer(parsed)
        self._schema = json.dumps(parsed) if parsed else None

    @staticmethod
    def _parse_json_schema(json_schema: str | None):
        if not json_schema:
            return None
        try:
            return json.loads(json_schema)
        except json.JSONDecodeError as e:
            raise KafkaErrorCode.KAFKA_14.exception(error=e)

    @staticmethod
    def _make_serializer(json_schema):
        def serialize(obj: dict, _ctx: "SerializationContext") -> bytes:
            if obj is None:
                raise ValueError("Serialisation failed: 'obj' is None")
            if json_schema:
                import jsonschema

                try:
                    jsonschema.validate(instance=obj, schema=json_schema)
                except jsonschema.ValidationError as e:
                    raise KafkaErrorCode.KAFKA_15.exception(error=e)
            return json.dumps(obj, separators=(",", ":")).encode("utf-8")

        return serialize

    @staticmethod
    def _make_deserializer(json_schema):
        def deserialize(data: bytes, _ctx: "SerializationContext") -> dict:
            if data is None:
                raise ValueError("Deserialisation failed: payload is None")
            try:
                record = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"Deserialisation failed: invalid JSON: {e}",
                ) from e
            if not isinstance(record, dict):
                raise ValueError(
                    f"Deserialisation failed: expected JSON object, got "
                    f"{type(record).__name__}"
                )
            if json_schema:
                import jsonschema

                try:
                    jsonschema.validate(instance=record, schema=json_schema)
                except jsonschema.ValidationError as e:
                    raise KafkaErrorCode.KAFKA_15.exception(error=e)
            return record

        return deserialize

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
        return self._deserializer

    def __repr__(self):
        return f"JsonKafkaSerde(schema={self._schema!r})"


class AvroKafkaSerde(NonSrKafkaSerde):
    """No-SR Avro. The schema is parsed once at construction and
    reused for every serialise / deserialise call."""

    def __init__(self, avro_schema: str):
        super().__init__()
        import avro.schema

        schema = avro.schema.parse(avro_schema)
        self._serializer = self._make_serializer(schema)
        self._deserializer = self._make_deserializer(schema)
        self._schema = avro_schema

    @staticmethod
    def _make_serializer(schema):
        import avro.io

        def serializer(obj: dict, _ctx: "SerializationContext") -> bytes:
            writer = avro.io.DatumWriter(schema)
            buf = io.BytesIO()
            encoder = avro.io.BinaryEncoder(buf)
            writer.write(obj, encoder)
            return buf.getvalue()

        return serializer

    @staticmethod
    def _make_deserializer(schema):
        import avro.io

        def deserializer(data: bytes, _ctx: "SerializationContext") -> dict:
            reader = avro.io.DatumReader(schema)
            decoder = avro.io.BinaryDecoder(io.BytesIO(data))
            return reader.read(decoder)

        return deserializer

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

    def __repr__(self):
        return f"AvroKafkaSerde(schema={self._schema!r})"


class ProtobufKafkaSerde(NonSrKafkaSerde):
    """No-SR Protobuf. The user-supplied ``.proto`` is compiled at
    construction; the resulting message class is reused for every
    serialise / deserialise."""

    def __init__(
        self,
        *,
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
        self._serializer = self._make_serializer(msg_class)
        self._deserializer = self._make_deserializer(msg_class)
        self._schema = protobuf_schema
        self._message_name = message_name
        self._working_dir = working_dir

    @staticmethod
    def _make_serializer(msg_class):
        from google.protobuf.json_format import ParseDict

        def serializer(obj_dict: dict, _ctx: "SerializationContext") -> bytes:
            message = msg_class()
            ParseDict(obj_dict, message)
            return message.SerializeToString()

        return serializer

    @staticmethod
    def _make_deserializer(msg_class):
        from google.protobuf.json_format import MessageToDict

        def deserializer(data: bytes, _ctx: "SerializationContext") -> dict:
            message = msg_class()
            message.ParseFromString(data)
            return MessageToDict(message, preserving_proto_field_name=True)

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

    def __repr__(self):
        return (
            f"ProtobufKafkaSerde(message_name={self._message_name!r}, "
            f"working_dir={self._working_dir!r})"
        )

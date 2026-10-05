#
# Copyright 2026 Tabsdata Inc.
#

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
    build_json_validator,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._protobuf import get_protobuf_class
from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover
    from confluent_kafka.serialization import SerializationContext


logger = logging.getLogger(__name__)

# The schema shape JsonKafkaSerde._fast_accept understands; any other keyword
# sends every record to the full validator. ``format`` is an annotation to a
# validator built without a format checker, as this one is.
_FAST_TOP_KEYS = frozenset({
    "type",
    "properties",
    "required",
    "additionalProperties",
    "title",
    "description",
    "$schema",
    "$id",
})
_FAST_PROPERTY_KEYS = frozenset({
    "type",
    "format",
    "title",
    "description",
    "default",
    "examples",
})
_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "string": (str,),
    "integer": (int,),
    "number": (int, float),
    "boolean": (bool,),
    "null": (type(None),),
    "array": (list,),
    "object": (dict,),
}


def _is_arrow(*names: str):
    def check(arrow_type) -> bool:
        import pyarrow.types as pat

        return any(getattr(pat, f"is_{name}")(arrow_type) for name in names)

    return check


# The JSON types a batch decodes, and the Arrow types each may come back as --
# the ones the per-message path infers from the same values.
_ARROW_TYPES = {
    "string": _is_arrow("string", "large_string"),
    "integer": _is_arrow("int64"),
    "number": _is_arrow("int64", "float64"),
    "boolean": _is_arrow("boolean"),
}


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
        validator = build_json_validator(parsed) if parsed else None
        accept = self._fast_accept(parsed) if validator else None
        self._serializer = self._make_serializer(validator)
        self._deserializer = self._make_deserializer(validator, accept)
        self._batch_decoder = self._make_batch_decoder(parsed) if validator else None
        self._schema = json.dumps(parsed) if parsed else None

    def batch_decoder(self):
        return self._batch_decoder

    @staticmethod
    def _make_batch_decoder(json_schema):
        """Decode a fetched batch into one Arrow table at once -- or None, and
        the batch goes message by message. Only for a schema the fast path
        understands whose properties are each one of string, integer, number
        or boolean; and only a batch whose every message carries exactly those
        properties, none null, each of its declared type. Such a batch is valid
        message by message, and the per-message path would build the same one
        table of it. Anything else -- a missing, extra or null field, another
        type, a message that is not a JSON object, a payload with a newline in
        it -- falls back, so validation errors and the dead-letter topic stay
        the per-message path's."""
        if JsonKafkaSerde._fast_accept(json_schema) is None:
            return None
        declared: dict[str, str] = {}
        for name, sub in json_schema.get("properties", {}).items():
            kind = sub.get("type")
            if not isinstance(kind, str) or kind not in _ARROW_TYPES:
                return None
            declared[name] = kind
        if not declared or set(json_schema.get("required", [])) - set(declared):
            return None
        import pyarrow as pa
        import pyarrow.json as pajson

        # Strings read as strings: inferred, an ISO date would become a timestamp.
        options = pajson.ParseOptions(
            explicit_schema=pa.schema([
                (name, pa.string())
                for name, kind in declared.items()
                if kind == "string"
            ]),
            unexpected_field_behavior="infer",
        )
        names = set(declared)

        def decode(values: list[bytes]):
            if not values:
                return None
            for value in values:
                if value is None or b"\n" in value:
                    return None
            try:
                table = pajson.read_json(
                    io.BytesIO(b"\n".join(values)), parse_options=options
                )
            except Exception:  # noqa: BLE001 -- the per-message path says why
                return None
            if table.num_rows != len(values) or set(table.column_names) != names:
                return None
            for name, kind in declared.items():
                column = table.column(name)
                if column.null_count or not _ARROW_TYPES[kind](column.type):
                    return None
            return table

        return decode

    @staticmethod
    def _parse_json_schema(json_schema: str | None):
        if not json_schema:
            return None
        try:
            return json.loads(json_schema)
        except json.JSONDecodeError as e:
            raise KafkaErrorCode.KAFKA_14.exception(error=e)

    @staticmethod
    def _fast_accept(json_schema):
        """A cheap check that ACCEPTS a record only when it is certainly valid,
        for the common schema shape -- an object of typed properties, some
        required -- or None for any other schema. A record it does not accept
        goes to the full validator, which then accepts it (``1.0`` for an
        ``integer``, say) or says why not, so errors are the validator's own.
        ``jsonschema`` walks such a schema property by property, ~20 us a
        message; this is a dict lookup and an ``isinstance`` per field."""
        if not isinstance(json_schema, dict) or json_schema.get("type") != "object":
            return None
        if set(json_schema) - _FAST_TOP_KEYS:
            return None
        if json_schema.get("additionalProperties", True) is not True:
            return None
        properties = json_schema.get("properties", {})
        required = json_schema.get("required", [])
        if not isinstance(properties, dict) or not isinstance(required, list):
            return None
        checks: list[tuple[str, tuple[type, ...]]] = []
        for name, sub in properties.items():
            if not isinstance(sub, dict) or set(sub) - _FAST_PROPERTY_KEYS:
                return None
            kinds = sub.get("type")
            if kinds is None:
                continue
            kinds = [kinds] if isinstance(kinds, str) else kinds
            if not isinstance(kinds, list) or any(k not in _JSON_TYPES for k in kinds):
                return None
            checks.append((name, tuple(t for k in kinds for t in _JSON_TYPES[k])))
        must = tuple(required)

        def accept(record: dict) -> bool:
            for name in must:
                if name not in record:
                    return False
            for name, types in checks:
                if name in record:
                    value = record[name]
                    # bool is an int to Python and never an integer to JSON Schema.
                    if not isinstance(value, types) or (
                        (value is True or value is False) and bool not in types
                    ):
                        return False
            return True

        return accept

    @staticmethod
    def _make_serializer(validator):
        def serialize(obj: dict, _ctx: "SerializationContext") -> bytes:
            if obj is None:
                raise ValueError("Serialisation failed: 'obj' is None")
            if validator is not None:
                import jsonschema

                try:
                    validator.validate(obj)
                except jsonschema.ValidationError as e:
                    raise KafkaErrorCode.KAFKA_15.exception(error=e)
            return json.dumps(obj, separators=(",", ":")).encode("utf-8")

        return serialize

    @staticmethod
    def _make_deserializer(validator, accept):
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
            if validator is not None and (accept is None or not accept(record)):
                import jsonschema

                try:
                    validator.validate(record)
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
            return MessageToDict(
                message,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )

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

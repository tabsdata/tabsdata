#
# Copyright 2026 Tabsdata Inc.
#

"""Confluent SR serdes: schema-id extraction + compat checker.

The serializer / deserializer callables wrap the confluent-kafka
SDK objects; we test the wire-header parsing, the compat
predicate and, for Protobuf, round trips through an in-memory
registry. Round-trip against a live registry is exercised by the
integration tests."""

from __future__ import annotations

import itertools
from unittest.mock import MagicMock, patch

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._confluent import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
    SrKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._protobuf import get_protobuf_class
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


# --- SrKafkaSerde base ---------------------------------------------


def _sr_serde():
    """Concrete instance to test the base behaviour. Mock-out the
    SR client so construction doesn't need a live registry."""
    return JsonSrKafkaSerde(
        schema_registry_client=MagicMock(),
        json_schema=None,
    )


def test_sr_extract_schema_id_parses_5_byte_header():
    serde = _sr_serde()
    # Magic byte 0x00 + big-endian schema id (42).
    payload = bytes([0x00, 0x00, 0x00, 0x00, 0x2A]) + b"...rest..."
    assert serde.extract_schema_id(payload, {}) == "42"


def test_sr_extract_schema_id_handles_high_id_values():
    serde = _sr_serde()
    sid = 0xDEADBEEF
    payload = bytes([0x00]) + sid.to_bytes(4, byteorder="big") + b""
    assert serde.extract_schema_id(payload, {}) == str(sid)


def test_sr_extract_schema_id_rejects_short_payload():
    serde = _sr_serde()
    with pytest.raises(TabsdataException) as ei:
        serde.extract_schema_id(b"\x00\x00\x00", {})
    assert ei.value.api_error is KafkaErrorCode.KAFKA_13


def test_sr_extract_schema_id_rejects_none_payload():
    serde = _sr_serde()
    with pytest.raises(TabsdataException) as ei:
        serde.extract_schema_id(None, {})  # type: ignore[arg-type]
    assert ei.value.api_error is KafkaErrorCode.KAFKA_13


def test_sr_compat_checker_is_exact_id_equality():
    check = _sr_serde().schema_id_compatibles_checker()
    assert check("42", "42") is True
    assert check("42", "43") is False


# --- Format labels -------------------------------------------------


def test_avro_sr_serde_format_label():
    serde = AvroSrKafkaSerde(
        schema_registry_client=MagicMock(),
        avro_schema='{"type":"record","name":"X","fields":[]}',
    )
    assert serde.format == "avro"


def test_json_sr_serde_format_label():
    serde = JsonSrKafkaSerde(schema_registry_client=MagicMock())
    assert serde.format == "json"


# --- ProtobufSrKafkaSerde shape ------------------------------------


_EVENTS_V1 = (
    'syntax = "proto3";\n'
    "package example;\n"
    "message Header { string source = 1; }\n"
    "message Event {\n"
    "  int32 id = 1;\n"
    "  string name = 2;\n"
    "  repeated string tags = 3;\n"
    "  Header header = 4;\n"
    "  int64 big = 5;\n"
    "  enum Kind { UNKNOWN = 0; CLICK = 1; }\n"
    "  Kind kind = 6;\n"
    "}\n"
)

_EVENTS_V2 = _EVENTS_V1.replace(
    "  Kind kind = 6;\n", "  Kind kind = 6;\n  string added = 7;\n"
)

_EVENTS_V0 = 'syntax = "proto3";\npackage example;\nmessage Event { int32 id = 1; }\n'


class _TextRegistry:
    """An in-memory Schema Registry that answers every schema read with the
    ``.proto`` text, whatever format is asked for -- as a registry that does
    not honour ``format=serialized`` does."""

    def __init__(self, text: str):
        self._text = text
        self._ids = itertools.count(11)
        self.calls: list[tuple] = []

    def config(self) -> dict:
        return {}

    def get_associations_by_resource_name(self, **_kwargs) -> list:
        return []

    def register_schema_full_response(self, subject, schema, normalize_schemas=False):
        from confluent_kafka.schema_registry import RegisteredSchema

        self.calls.append(("register", subject))
        return RegisteredSchema(
            schema_id=next(self._ids),
            guid=None,
            schema=schema,
            subject=subject,
            version=1,
        )

    def get_schema(self, schema_id, subject_name=None, fmt=None, reference_format=None):
        from confluent_kafka.schema_registry import Schema

        self.calls.append(("get", schema_id, fmt))
        return Schema(self._text, "PROTOBUF")


def _proto_sr_serde(
    tmp_path, schema: str, registry=None, *, folder: str = "serde"
) -> ProtobufSrKafkaSerde:
    working_dir = tmp_path / folder
    working_dir.mkdir()
    return ProtobufSrKafkaSerde(
        schema_registry_client=registry or _TextRegistry(schema),
        protobuf_schema=schema,
        message_name="example.Event",
        working_dir=str(working_dir),
    )


def _value_ctx():
    from confluent_kafka.serialization import MessageField, SerializationContext

    return SerializationContext("events", MessageField.VALUE)


def test_protobuf_sr_round_trips_through_the_confluent_wire_format(tmp_path):
    registry = _TextRegistry(_EVENTS_V1)
    serde = _proto_sr_serde(tmp_path, _EVENTS_V1, registry)
    record = {
        "id": 7,
        "name": "ann",
        "tags": ["a", "b"],
        "header": {"source": "web"},
        "big": "9007199254740993",
        "kind": "CLICK",
    }

    data = serde.serializer()(record, _value_ctx())

    assert data[0] == 0
    assert serde.extract_schema_id(data, {}) == "11"
    assert serde.deserializer()(data, _value_ctx()) == record
    assert registry.calls == [("register", "events-value")]


def test_protobuf_sr_decodes_a_message_that_is_not_first_in_its_file(tmp_path):
    serde = _proto_sr_serde(tmp_path, _EVENTS_V1)

    data = serde.serializer()({"id": 3}, _value_ctx())

    # Message indexes follow the schema id: one index, of value 1 (zigzag).
    assert data[5:7] == b"\x02\x02"
    assert serde.deserializer()(data, _value_ctx())["id"] == 3


def test_protobuf_sr_decodes_when_the_registry_answers_with_proto_text(tmp_path):
    """The registry read the confluent deserializer makes for its own rules is
    what fails here (TD-2809), so the serde must decode without it."""
    from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
    from confluent_kafka.serialization import SerializationError

    registry = _TextRegistry(_EVENTS_V1)
    serde = _proto_sr_serde(tmp_path, _EVENTS_V1, registry)
    data = serde.serializer()({"id": 7, "name": "ann"}, _value_ctx())
    (tmp_path / "reader").mkdir()
    event = get_protobuf_class(
        working_dir=str(tmp_path / "reader"),
        protobuf_schema=_EVENTS_V1,
        message_name="example.Event",
    )

    with pytest.raises(SerializationError, match="FileDescriptorProto"):
        ProtobufDeserializer(message_type=event, schema_registry_client=registry)(
            data, _value_ctx()
        )
    assert ("get", 11, "serialized") in registry.calls

    registry.calls.clear()
    assert serde.deserializer()(data, _value_ctx())["name"] == "ann"
    assert registry.calls == []


def test_protobuf_sr_reads_messages_of_a_newer_writer_schema(tmp_path):
    writer = _proto_sr_serde(tmp_path, _EVENTS_V2, folder="writer")
    reader = _proto_sr_serde(tmp_path, _EVENTS_V1, folder="reader")

    data = writer.serializer()({"id": 7, "added": "new"}, _value_ctx())
    decoded = reader.deserializer()(data, _value_ctx())

    assert decoded["id"] == 7
    assert "added" not in decoded


def test_protobuf_sr_reads_messages_of_an_older_writer_schema(tmp_path):
    writer = _proto_sr_serde(tmp_path, _EVENTS_V0, folder="writer")
    reader = _proto_sr_serde(tmp_path, _EVENTS_V1, folder="reader")

    data = writer.serializer()({"id": 7}, _value_ctx())
    decoded = reader.deserializer()(data, _value_ctx())

    assert decoded["id"] == 7
    assert decoded["name"] == ""
    assert decoded["tags"] == []


def test_protobuf_sr_refuses_a_payload_without_the_confluent_framing(tmp_path):
    from confluent_kafka.serialization import SerializationError

    serde = _proto_sr_serde(tmp_path, _EVENTS_V1)
    data = serde.serializer()({"id": 7}, _value_ctx())

    with pytest.raises(SerializationError, match="magic byte"):
        serde.deserializer()(b"\x05" + data[1:], _value_ctx())


def test_protobuf_sr_default_values_are_kept(tmp_path):
    event = get_protobuf_class(
        working_dir=str(tmp_path),
        protobuf_schema=(
            'syntax = "proto3";\n'
            'package example;\n'
            'message Event { int32 id = 1; string name = 2; }\n'
        ),
        message_name="example.Event",
    )
    with patch(
        "confluent_kafka.schema_registry.protobuf.ProtobufDeserializer",
        return_value=lambda _data, _ctx: event(),
    ):
        deserializer = ProtobufSrKafkaSerde._make_deserializer(event)
    assert deserializer(b"", None) == {"id": 0, "name": ""}

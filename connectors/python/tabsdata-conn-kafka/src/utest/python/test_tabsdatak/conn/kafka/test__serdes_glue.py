"""AWS Glue SR serdes: wire-header parsing + round-trips against
a mocked boto3 ``glue`` client."""

from __future__ import annotations

import json
import uuid
from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._glue import (
    AvroGlueKafkaSerde,
    GlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


# --- Stub boto3 glue client -----------------------------------------


_FIXED_SCHEMA_UUID = uuid.UUID("00000000-0000-0000-0000-000000000042")


def _fake_glue_client(schema_definition: str | None = None) -> MagicMock:
    """Stub a boto3 Glue client that resolves
    ``get_schema_by_definition`` to a deterministic UUID and
    ``get_schema_version`` back to the schema definition."""
    client = MagicMock()
    client.exceptions.EntityNotFoundException = type(
        "EntityNotFoundException", (Exception,), {},
    )
    client.get_schema_by_definition.return_value = {
        "SchemaVersionId": str(_FIXED_SCHEMA_UUID),
    }
    if schema_definition is not None:
        client.get_schema_version.return_value = {
            "SchemaDefinition": schema_definition,
        }
    return client


# --- GlueKafkaSerde base -------------------------------------------


def _glue_serde():
    return JsonGlueKafkaSerde(
        glue_client=_fake_glue_client(json.dumps({"type": "object"})),
        registry_name="reg",
        schema_name="sch",
        json_schema=json.dumps({"type": "object"}),
        validate_schema=False,
    )


def test_glue_extract_schema_id_parses_18_byte_header():
    serde = _glue_serde()
    # Header: [ver=3][comp=0][uuid(16)] + payload bytes.
    payload = (
        bytes([3, 0]) + _FIXED_SCHEMA_UUID.bytes + b'{"id":1}'
    )
    assert serde.extract_schema_id(payload, {}) == str(_FIXED_SCHEMA_UUID)


def test_glue_extract_schema_id_rejects_short_payload():
    serde = _glue_serde()
    with pytest.raises(TabsdataException) as ei:
        serde.extract_schema_id(b"\x03\x00short", {})
    assert ei.value.api_error is KafkaErrorCode.KAFKA_13


def test_glue_compat_checker_is_exact_uuid_equality():
    check = _glue_serde().schema_id_compatibles_checker()
    same = str(_FIXED_SCHEMA_UUID)
    other = "00000000-0000-0000-0000-000000000099"
    assert check(same, same) is True
    assert check(same, other) is False


# --- JsonGlueKafkaSerde round-trip ----------------------------------


def test_json_glue_serialise_prepends_18_byte_header():
    schema = json.dumps({"type": "object"})
    serde = JsonGlueKafkaSerde(
        glue_client=_fake_glue_client(schema),
        registry_name="reg",
        schema_name="sch",
        json_schema=schema,
        validate_schema=False,
    )
    payload = serde.serializer()({"id": 1}, None)
    assert payload[0] == 3  # header version
    assert payload[1] == 0  # default compression
    assert payload[2:18] == _FIXED_SCHEMA_UUID.bytes
    assert json.loads(payload[18:]) == {"id": 1}


def test_json_glue_deserialise_round_trip():
    schema = json.dumps({"type": "object"})
    serde = JsonGlueKafkaSerde(
        glue_client=_fake_glue_client(schema),
        registry_name="reg",
        schema_name="sch",
        json_schema=schema,
        validate_schema=False,
    )
    payload = serde.serializer()({"id": 42, "name": "alice"}, None)
    record = serde.deserializer()(payload, None)
    assert record == {"id": 42, "name": "alice"}


def test_json_glue_validate_enforced_when_flag_true():
    schema = json.dumps({
        "type": "object",
        "required": ["id"],
    })
    serde = JsonGlueKafkaSerde(
        glue_client=_fake_glue_client(schema),
        registry_name="reg",
        schema_name="sch",
        json_schema=schema,
        validate_schema=True,
    )
    # A serialised record missing ``id`` -- caller's responsibility
    # to not produce it; if the broker delivered one we'd reject.
    bad_payload = serde.serializer()({"name": "no-id"}, None)
    import jsonschema

    with pytest.raises(jsonschema.ValidationError):
        serde.deserializer()(bad_payload, None)


def test_json_glue_format_label():
    serde = _glue_serde()
    assert serde.format == "json"


# --- AvroGlueKafkaSerde round-trip ---------------------------------


_AVRO_SCHEMA_STR = json.dumps({
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
})


def test_avro_glue_round_trip():
    serde = AvroGlueKafkaSerde(
        glue_client=_fake_glue_client(_AVRO_SCHEMA_STR),
        registry_name="reg",
        schema_name="sch",
        avro_schema=_AVRO_SCHEMA_STR,
    )
    payload = serde.serializer()({"id": 1, "name": "alice"}, None)
    assert payload[0] == 3
    assert payload[2:18] == _FIXED_SCHEMA_UUID.bytes
    record = serde.deserializer()(payload, None)
    assert record == {"id": 1, "name": "alice"}


def test_avro_glue_format_label():
    serde = AvroGlueKafkaSerde(
        glue_client=_fake_glue_client(_AVRO_SCHEMA_STR),
        registry_name="reg",
        schema_name="sch",
        avro_schema=_AVRO_SCHEMA_STR,
    )
    assert serde.format == "avro"


# --- ProtobufGlueKafkaSerde shape ----------------------------------


_PROTO_SCHEMA = (
    'syntax = "proto3";\n'
    'package example;\n'
    'message Event { int32 id = 1; string name = 2; }\n'
)


def test_protobuf_glue_round_trip(tmp_path):
    serde = ProtobufGlueKafkaSerde(
        glue_client=_fake_glue_client(_PROTO_SCHEMA),
        registry_name="reg",
        schema_name="sch",
        protobuf_schema=_PROTO_SCHEMA,
        message_name="example.Event",
        working_dir=str(tmp_path),
    )
    payload = serde.serializer()({"id": 7, "name": "bob"}, None)
    assert payload[0] == 3
    assert payload[2:18] == _FIXED_SCHEMA_UUID.bytes
    record = serde.deserializer()(payload, None)
    assert record == {"id": 7, "name": "bob"}


def test_protobuf_glue_format_label(tmp_path):
    serde = ProtobufGlueKafkaSerde(
        glue_client=_fake_glue_client(_PROTO_SCHEMA),
        registry_name="reg",
        schema_name="sch",
        protobuf_schema=_PROTO_SCHEMA,
        message_name="example.Event",
        working_dir=str(tmp_path),
    )
    assert serde.format == "protobuf"

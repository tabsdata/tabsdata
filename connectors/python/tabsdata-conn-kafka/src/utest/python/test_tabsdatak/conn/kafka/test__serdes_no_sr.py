"""No-schema-registry serdes: ``JsonKafkaSerde`` /
``AvroKafkaSerde`` / ``ProtobufKafkaSerde``."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._no_sr import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    NonSrKafkaSerde,
    ProtobufKafkaSerde,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


# --- JsonKafkaSerde -------------------------------------------------


def test_json_format_label():
    assert JsonKafkaSerde().format == "json"


def test_json_serialise_round_trip_no_schema():
    serde = JsonKafkaSerde()
    payload = serde.serializer()({"id": 1, "name": "x"}, None)
    record, schema_id = serde.deserializer_with_schema_id()(payload, None)
    assert record == {"id": 1, "name": "x"}
    assert schema_id == ("id", "name")


def test_json_compat_checker_accepts_subset_keys():
    check = JsonKafkaSerde().schema_id_compatibles_checker()
    assert check(("id", "name"), ("id",)) is True
    assert check(("id", "name"), ("id", "name", "age")) is False


def test_json_rejects_none_payload():
    serde = JsonKafkaSerde()
    with pytest.raises(ValueError):
        serde.deserializer()(None, None)


def test_json_rejects_non_object_payload():
    serde = JsonKafkaSerde()
    with pytest.raises(ValueError):
        serde.deserializer()(b"[1, 2, 3]", None)


def test_json_schema_validation_enforced_on_deserialise():
    schema = json.dumps({
        "type": "object",
        "properties": {"id": {"type": "integer"}},
        "required": ["id"],
        "additionalProperties": False,
    })
    serde = JsonKafkaSerde(json_schema=schema)
    # Valid record round-trips.
    record = serde.deserializer()(b'{"id": 1}', None)
    assert record == {"id": 1}
    # Invalid record (missing required ``id``) trips KAFKA_15.
    with pytest.raises(TabsdataException) as ei:
        serde.deserializer()(b'{"name": "no-id"}', None)
    assert ei.value.api_error is KafkaErrorCode.KAFKA_15


def test_json_schema_validation_enforced_on_serialise():
    schema = json.dumps({
        "type": "object",
        "required": ["id"],
    })
    serde = JsonKafkaSerde(json_schema=schema)
    with pytest.raises(TabsdataException) as ei:
        serde.serializer()({}, None)
    assert ei.value.api_error is KafkaErrorCode.KAFKA_15


def test_json_serde_rejects_malformed_schema_string():
    with pytest.raises(TabsdataException) as ei:
        JsonKafkaSerde(json_schema="{not valid json")
    assert ei.value.api_error is KafkaErrorCode.KAFKA_14


# --- AvroKafkaSerde -------------------------------------------------


_AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
})


def test_avro_format_label():
    assert AvroKafkaSerde(avro_schema=_AVRO_SCHEMA).format == "avro"


def test_avro_round_trip():
    serde = AvroKafkaSerde(avro_schema=_AVRO_SCHEMA)
    payload = serde.serializer()({"id": 7, "name": "alice"}, None)
    assert isinstance(payload, bytes)
    record, schema_id = serde.deserializer_with_schema_id()(payload, None)
    assert record == {"id": 7, "name": "alice"}
    # No-SR schema id is the sorted field-name tuple.
    assert schema_id == ("id", "name")


def test_avro_compat_checker_inherits_subset_rule():
    check = AvroKafkaSerde(
        avro_schema=_AVRO_SCHEMA,
    ).schema_id_compatibles_checker()
    assert check(("id", "name"), ("id",)) is True
    assert check(("id",), ("id", "name")) is False


# --- ProtobufKafkaSerde --------------------------------------------


_PROTO_SCHEMA = """\
syntax = "proto3";
package example;
message Event {
    int32 id = 1;
    string name = 2;
}
"""


def test_protobuf_format_label(tmp_path):
    serde = ProtobufKafkaSerde(
        protobuf_schema=_PROTO_SCHEMA,
        message_name="example.Event",
        working_dir=str(tmp_path),
    )
    assert serde.format == "protobuf"


def test_protobuf_round_trip(tmp_path):
    serde = ProtobufKafkaSerde(
        protobuf_schema=_PROTO_SCHEMA,
        message_name="example.Event",
        working_dir=str(tmp_path),
    )
    payload = serde.serializer()({"id": 9, "name": "bob"}, None)
    assert isinstance(payload, bytes)
    record, schema_id = serde.deserializer_with_schema_id()(payload, None)
    assert record == {"id": 9, "name": "bob"}
    # No-SR schema id is the sorted field-name tuple.
    assert schema_id == ("id", "name")


def test_protobuf_unknown_message_name_raises_kafka_17(tmp_path):
    with pytest.raises(TabsdataException) as ei:
        ProtobufKafkaSerde(
            protobuf_schema=_PROTO_SCHEMA,
            message_name="example.DoesNotExist",
            working_dir=str(tmp_path),
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_17


# --- NonSrKafkaSerde common base ------------------------------------


def test_non_sr_base_extract_schema_id_returns_sorted_keys():
    serde = JsonKafkaSerde()
    assert serde.extract_schema_id(b"", {"b": 2, "a": 1}) == ("a", "b")

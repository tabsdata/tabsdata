#
# Copyright 2026 Tabsdata Inc.
#

"""Confluent SR serdes: schema-id extraction + compat checker.

The serializer / deserializer callables wrap the confluent-kafka
SDK objects; we test the wire-header parsing and the compat
predicate. Round-trip against a real SR client is exercised by
the integration tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._confluent import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
    SrKafkaSerde,
)
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


def test_protobuf_sr_serde_takes_separate_serializer_deserializer_clients(
    tmp_path,
):
    """Protobuf SR needs two SR clients because Confluent's client
    caches schemas by id and the serializer cache (proto-text)
    diverges from the deserializer cache (binary). The constructor
    must accept both."""
    serializer_client = MagicMock()
    deserializer_client = MagicMock()
    serde = ProtobufSrKafkaSerde(
        serializer_registry_client=serializer_client,
        deserializer_registry_client=deserializer_client,
        protobuf_schema=(
            'syntax = "proto3";\n'
            'package example;\n'
            'message Event { int32 id = 1; }\n'
        ),
        message_name="example.Event",
        working_dir=str(tmp_path),
    )
    assert serde.format == "protobuf"

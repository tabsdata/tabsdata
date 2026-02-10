#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from confluent_kafka.serialization import MessageField, SerializationContext

# noinspection PyUnusedImports
from test_tabsdata_kafka.conftest import (
    confluent_sr_avro_serde,
    confluent_sr_json_serde,
    confluent_sr_mock_registry,
    confluent_sr_protobuf_serde,
)


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_serializer_deserializer(confluent_sr_avro_serde):
    """
    Tests the serialization and deserialization of a message using AvroSrKafkaSerde.
    """
    confluent_sr_avro_serde = confluent_sr_avro_serde(None, mock=True)
    assert confluent_sr_avro_serde.format == "avro"

    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}

    # Create a dummy SerializationContext
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_avro_serde.serializer()
    deserializer = confluent_sr_avro_serde.deserializer()

    # Test serialization
    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    # Test deserialization
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_deserializer_with_schema_id(confluent_sr_avro_serde):
    """
    Tests the deserialization of a message with schema id using AvroSrKafkaSerde.
    """
    confluent_sr_avro_serde = confluent_sr_avro_serde(None, mock=True)
    test_data = {"name": "test_user", "favorite_number": 456, "favorite_color": "red"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_avro_serde.serializer()
    deserializer = confluent_sr_avro_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == "1"


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serializer_deserializer(confluent_sr_json_serde):
    """
    Tests the serialization and deserialization of a message using JsonSrKafkaSerde.
    """
    confluent_sr_json_serde = confluent_sr_json_serde(None, mock=True)
    assert confluent_sr_json_serde.format == "json"

    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}

    # Create a dummy SerializationContext
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_json_serde.serializer()
    deserializer = confluent_sr_json_serde.deserializer()

    # Test serialization
    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    # Test deserialization
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_json_deserializer_with_schema_id(confluent_sr_json_serde):
    """
    Tests the deserialization of a message with schema id using JsonSrKafkaSerde.
    """
    confluent_sr_json_serde = confluent_sr_json_serde(None, mock=True)
    test_data = {"name": "test_user", "favorite_number": 456, "favorite_color": "red"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_json_serde.serializer()
    deserializer = confluent_sr_json_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == "1"


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_serializer_deserializer(confluent_sr_protobuf_serde):
    """
    Tests the serialization and deserialization of a message using ProtobufSrKafkaSerde.
    """
    confluent_sr_protobuf_serde = confluent_sr_protobuf_serde(None, mock=True)
    assert confluent_sr_protobuf_serde.format == "protobuf"

    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}

    # Create a dummy SerializationContext
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_protobuf_serde.serializer()
    deserializer = confluent_sr_protobuf_serde.deserializer()

    # Test serialization
    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    # Test deserialization
    deserialized_data = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_deserializer_with_schema_id(confluent_sr_protobuf_serde):
    """
    Tests the deserialization of a message with schema id using ProtobufSrKafkaSerde.
    """
    confluent_sr_protobuf_serde = confluent_sr_protobuf_serde(None, mock=True)
    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = confluent_sr_protobuf_serde.serializer()
    deserializer = confluent_sr_protobuf_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == "1"

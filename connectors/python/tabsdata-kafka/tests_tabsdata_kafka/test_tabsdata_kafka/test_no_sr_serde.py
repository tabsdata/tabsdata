#
# Copyright 2026 Tabs Data Inc.
#

import json

import pytest
from confluent_kafka.serialization import MessageField, SerializationContext

from tabsdata_kafka.serdes.non_sr_serde import JsonKafkaSerde
from tests_tabsdata_kafka.conftest import (
    avro_serde_v1,
    avro_serde_v2,
    json_serde,
    json_serde_no_schema,
    protobuf_serde_v1,
    protobuf_serde_v2,
)

# JSON Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_invalid_schema_string_raises_error():
    """
    Tests that providing an invalid JSON schema string to the constructor raises a ValueError.
    """
    invalid_schema_string = "{'type': 'object', 'properties': 'invalid'}"
    with pytest.raises(
        ValueError,
        match=(
            "Invalid JSON schema string provided: Expecting property name enclosed in"
            " double quotes"
        ),
    ):
        JsonKafkaSerde(json_schema=invalid_schema_string)


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_serialization_deserialization_with_schema(json_serde):
    """
    Tests successful serialization and deserialization of a JSON message that conforms to the schema.
    """

    assert json_serde.format == "json"

    original_data = {"name": "test", "age": 30}  # 'city' is not required by schema

    serializer = json_serde.serializer()
    deserializer = json_serde.deserializer()

    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    # Serialize
    serialized_data = serializer(original_data, serialization_ctx)
    assert isinstance(serialized_data, bytes)
    assert json.loads(serialized_data.decode("utf-8")) == original_data

    # Deserialize
    deserialized_data = deserializer(serialized_data, serialization_ctx)
    assert isinstance(deserialized_data, dict)
    assert deserialized_data == original_data


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_serialization_deserialization_with_no_schema(json_serde_no_schema):
    """
    Tests successful serialization and deserialization of a JSON message that conforms to the schema.
    """

    assert json_serde_no_schema.format == "json"

    original_data = {"name": "test", "age": 30}  # 'city' is not required by schema

    serializer = json_serde_no_schema.serializer()
    deserializer = json_serde_no_schema.deserializer()

    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    # Serialize
    serialized_data = serializer(original_data, serialization_ctx)
    assert isinstance(serialized_data, bytes)
    assert json.loads(serialized_data.decode("utf-8")) == original_data

    # Deserialize
    deserialized_data = deserializer(serialized_data, serialization_ctx)
    assert isinstance(deserialized_data, dict)
    assert deserialized_data == original_data


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_serialization_invalid_message_raises_error(json_serde):
    """
    Tests that serializing a message that does not conform to the schema raises a ValueError.
    """
    invalid_data = {"name": "test", "age": -5}  # age must be >= 0
    serializer = json_serde.serializer()

    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    with pytest.raises(
        ValueError,
        match=(
            "Serialization failed: Message does not conform to schema: -5 is less than"
            " the minimum of 0"
        ),
    ):
        serializer(invalid_data, serialization_ctx)

    invalid_data_missing_required = {"name": "test"}  # missing age
    with pytest.raises(
        ValueError,
        match=(
            "Serialization failed: Message does not conform to schema: 'age' is a"
            " required property"
        ),
    ):
        serializer(invalid_data_missing_required, serialization_ctx)

    invalid_data_additional_properties = {
        "name": "test",
        "age": 30,
        "extra_field": "value",
    }  # additionalProperties is False
    with pytest.raises(
        ValueError,
        match=(
            r"Serialization failed: Message does not conform to schema: Additional"
            r" properties are not allowed \('extra_field' was unexpected\)"
        ),
    ):
        serializer(invalid_data_additional_properties, serialization_ctx)


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_deserialization_invalid_message_raises_error(json_serde):
    """
    Tests that deserializing a message that does not conform to the schema raises a ValueError.
    """
    # Invalid data (age is negative)
    invalid_json_bytes = b'{"name":"test","age":-5}'
    deserializer = json_serde.deserializer()

    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    with pytest.raises(
        ValueError,
        match=(
            "Deserialization failed: Message does not conform to schema: -5 is less"
            " than the minimum of 0"
        ),
    ):
        deserializer(invalid_json_bytes, serialization_ctx)

    # Invalid data (missing required field)
    invalid_json_bytes = b'{"name":"test"}'
    with pytest.raises(
        ValueError,
        match=(
            "Deserialization failed: Message does not conform to schema: 'age' is a"
            " required property"
        ),
    ):
        deserializer(invalid_json_bytes, serialization_ctx)

    # Invalid data (additional property)
    invalid_json_bytes = b'{"name":"test","age":30,"extra_field":"value"}'
    with pytest.raises(
        ValueError,
        match=(
            r"Deserialization failed: Message does not conform to schema: Additional"
            r" properties are not allowed \('extra_field' was unexpected\)"
        ),
    ):
        deserializer(invalid_json_bytes, serialization_ctx)


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_empty_dict(json_serde):
    """
    Tests serialization and deserialization of an empty dictionary.
    This should fail with the current schema as 'name' and 'age' are required.
    """
    original_data = {}
    serializer = json_serde.serializer()
    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    with pytest.raises(
        ValueError,
        match=(
            "Serialization failed: Message does not conform to schema: 'name' is a"
            " required property"
        ),
    ):
        serializer(original_data, serialization_ctx)

    deserializer = json_serde.deserializer()
    with pytest.raises(
        ValueError,
        match=(
            "Deserialization failed: Message does not conform to schema: 'name' is a"
            " required property"
        ),
    ):
        deserializer(b"{}", serialization_ctx)


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_complex_data_with_schema(json_serde):
    """
    Tests serialization and deserialization of more complex JSON data that conforms to the schema.
    Note: The PERSON_SCHEMA is simple, so this test uses data that fits it.
    """
    original_data = {"name": "complex_test", "age": 45}

    serializer = json_serde.serializer()
    deserializer = json_serde.deserializer()
    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    serialized_data = serializer(original_data, serialization_ctx)
    assert isinstance(serialized_data, bytes)
    assert json.loads(serialized_data.decode("utf-8")) == original_data

    deserialized_data = deserializer(serialized_data, serialization_ctx)
    assert isinstance(deserialized_data, dict)
    assert deserialized_data == original_data


@pytest.mark.unit
@pytest.mark.kafka
def test_json_serde_deserialization_invalid_json_format(json_serde):
    """
    Tests that deserializing an invalid JSON string raises a ValueError due to JSONDecodeError.
    """
    invalid_json_bytes = b'{"name":"test","age":30,'  # Incomplete JSON
    deserializer = json_serde.deserializer()
    serialization_ctx = SerializationContext("test_topic", MessageField.VALUE)

    with pytest.raises(
        ValueError,
        match=(
            "Deserialization failed: Invalid JSON format: Expecting property name"
            " enclosed in double quotes"
        ),
    ):
        deserializer(invalid_json_bytes, serialization_ctx)

    invalid_json_bytes = b"not json"
    with pytest.raises(
        ValueError,
        match=(
            "Deserialization failed: Invalid JSON format: Expecting value: line 1"
            " column 1 \(char 0\)"
        ),
    ):
        deserializer(invalid_json_bytes, serialization_ctx)


PERSON_SCHEMA_V1_COMPATIBLE = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0},
    },
    "required": ["name", "age"],
    "additionalProperties": (
        True
    ),  # Allow additional properties for backward compatibility
}

PERSON_SCHEMA_V2_COMPATIBLE = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "age": {"type": "integer", "minimum": 0},
        "city": {"type": "string"},  # New optional field
    },
    "required": ["name", "age"],
    "additionalProperties": True,
}


@pytest.mark.unit
@pytest.mark.kafka
def test_json_backward_compatibility():
    """
    Tests backward compatibility for JSON schemas.
    A V1 consumer should be able to read a message from a V2 producer
    if the V1 schema is configured to allow additional properties.
    """
    serde_v1 = JsonKafkaSerde(json.dumps(PERSON_SCHEMA_V1_COMPATIBLE))
    serde_v2 = JsonKafkaSerde(json.dumps(PERSON_SCHEMA_V2_COMPATIBLE))
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    # Data from a V2 producer
    data_v2 = {"name": "test", "age": 30, "city": "New York"}

    # V2 serializes the data
    serializer_v2 = serde_v2.serializer()
    serialized_data = serializer_v2(data_v2, ctx)

    # V1 deserializes the data
    deserializer_v1 = serde_v1.deserializer()
    deserialized_data = deserializer_v1(serialized_data, ctx)

    # V1 schema allows additional properties, so the 'city' field is kept.
    assert deserialized_data == data_v2


@pytest.mark.unit
@pytest.mark.kafka
def test_json_deserializer_with_schema_id(json_serde):
    """
    Tests the deserialization of a message with schema id using JsonKafkaSerde.
    """
    test_data = {"name": "test", "age": 30}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = json_serde.serializer()
    deserializer = json_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == sorted(list(test_data.keys()))


# Avro Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_serializer_deserializer(avro_serde_v1):
    """
    Tests the serialization and deserialization of a message using AvroKafkaSerde.
    """

    assert avro_serde_v1.format == "avro"

    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}

    # Create a dummy SerializationContext
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = avro_serde_v1.serializer()
    deserializer = avro_serde_v1.deserializer()

    # Test serialization
    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    # Test deserialization
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_serializer_deserializer_null_fields(avro_serde_v1):
    """
    Tests serialization and deserialization with null fields.
    """
    test_data = {
        "name": "another_user",
        "favorite_number": None,
        "favorite_color": None,
    }

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = avro_serde_v1.serializer()
    deserializer = avro_serde_v1.deserializer()

    serialized_data = serializer(test_data, ctx)
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_serializer_deserializer_missing_optional_field(avro_serde_v1):
    """
    Tests serialization and deserialization when an optional field is missing.
    Avro's default behavior for missing fields is to use the default value if defined,
    or raise an error if not. Our schema defines null as a type, so missing fields
    should be treated as null if not provided.
    """
    test_data = {"name": "user_without_color", "favorite_number": 456}
    expected_data = {
        "name": "user_without_color",
        "favorite_number": 456,
        "favorite_color": None,
    }

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = avro_serde_v1.serializer()
    deserializer = avro_serde_v1.deserializer()

    serialized_data = serializer(test_data, ctx)
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == expected_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_backward_compatibility(avro_serde_v1, avro_serde_v2):
    """
    Tests for backward compatibility: a consumer with an older schema (v1)
    can read a message produced with a newer schema (v2).
    """
    # Data conforming to the new schema (v2)
    test_data_v2 = {
        "name": "test_user_v2",
        "favorite_number": 789,
        "favorite_color": "green",
        "address": "123 Main St",
    }

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    # Serializer from the new schema (v2)
    serializer_v2 = avro_serde_v2.serializer()
    # Deserializer from the old schema (v1)
    deserializer_v1 = avro_serde_v1.deserializer()

    # Serialize with v2
    serialized_data = serializer_v2(test_data_v2, ctx)

    # Deserialize with v1
    deserialized_data = deserializer_v1(serialized_data, ctx)

    # The 'address' field should be ignored by the v1 deserializer
    expected_data = {
        "name": "test_user_v2",
        "favorite_number": 789,
        "favorite_color": "green",
    }
    assert deserialized_data == expected_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_deserializer_with_schema_id(avro_serde_v1):
    """
    Tests the deserialization of a message with schema id using AvroKafkaSerde.
    """
    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = avro_serde_v1.serializer()
    deserializer = avro_serde_v1.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == sorted(list(test_data.keys()))


# Protobuf Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_serializer_deserializer(protobuf_serde_v1):
    """
    Tests the serialization and deserialization of a message using ProtobufKafkaSerde.
    """

    assert protobuf_serde_v1.format == "protobuf"

    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}

    # Create a dummy SerializationContext
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = protobuf_serde_v1.serializer()
    deserializer = protobuf_serde_v1.deserializer()

    # Test serialization
    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    # Test deserialization
    deserialized_data = deserializer(serialized_data, ctx)
    # Protobuf deserialization to dict might add default values for missing fields
    # or change field names if not preserving_proto_field_name=True.
    # Our implementation uses preserving_proto_field_name=True, so it should match.
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_serializer_deserializer_partial_data(protobuf_serde_v1):
    """
    Tests serialization and deserialization with partial data (missing optional fields).
    Protobuf will use default values for missing fields.
    """
    test_data = {"name": "Jane Smith", "id": 456}
    expected_data = {
        "name": "Jane Smith",
        "id": 456,
    }  # email is string, default is empty string, so it's omitted by MessageToDict

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = protobuf_serde_v1.serializer()
    deserializer = protobuf_serde_v1.deserializer()

    serialized_data = serializer(test_data, ctx)
    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == expected_data


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_backward_compatibility(protobuf_serde_v1, protobuf_serde_v2):
    """
    Tests backward compatibility for Protobuf: a consumer with an older schema (v1)
    can read a message produced with a newer schema (v2).
    """
    # Data conforming to the new schema (v2)
    test_data_v2 = {
        "name": "John Doe",
        "id": 123,
        "email": "john.doe@example.com",
        "phone_number": "123-456-7890",
    }

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    # Serializer from the new schema (v2)
    serializer_v2 = protobuf_serde_v2.serializer()
    # Deserializer from the old schema (v1)
    deserializer_v1 = protobuf_serde_v1.deserializer()

    # Serialize with v2
    serialized_data = serializer_v2(test_data_v2, ctx)

    # Deserialize with v1
    deserialized_data = deserializer_v1(serialized_data, ctx)

    test_data_v1 = {
        "name": "John Doe",
        "id": 123,
        "email": "john.doe@example.com",
    }
    assert deserialized_data == test_data_v1


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_deserializer_with_schema_id(protobuf_serde_v1):
    """
    Tests the deserialization of a message with schema id using ProtobufKafkaSerde.
    """
    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = protobuf_serde_v1.serializer()
    deserializer = protobuf_serde_v1.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == sorted(list(test_data.keys()))

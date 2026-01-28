#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from confluent_kafka.serialization import MessageField, SerializationContext

# noinspection PyUnusedImports
# noinspection PyUnusedImports
# noinspection PyUnusedImports
# noinspection PyUnusedImports
# noinspection PyUnusedImports
# noinspection PyUnusedImports
from tests_tabsdata_kafka.conftest import (
    aws_glue_sr_avro_glue_serde,
    aws_glue_sr_avro_schema_str,
    aws_glue_sr_json_glue_serde,
    aws_glue_sr_json_schema_str,
    aws_glue_sr_mock_avro_glue_client,
    aws_glue_sr_mock_json_glue_client,
    aws_glue_sr_mock_protobuf_glue_client,
    aws_glue_sr_protobuf_glue_serde,
    aws_glue_sr_protobuf_schema_str,
)

# JSON Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_json_glue_serializer_deserializer(aws_glue_sr_json_glue_serde):
    assert aws_glue_sr_json_glue_serde.format() == "json"

    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_json_glue_serde.serializer()
    deserializer = aws_glue_sr_json_glue_serde.deserializer()

    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_json_glue_deserializer_with_schema_id(
    aws_glue_sr_json_glue_serde, aws_glue_sr_mock_json_glue_client
):
    _, schema_version_id = aws_glue_sr_mock_json_glue_client
    test_data = {"name": "test_user", "favorite_number": 456, "favorite_color": "red"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_json_glue_serde.serializer()
    deserializer = aws_glue_sr_json_glue_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == schema_version_id


# Avro Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_glue_serializer_deserializer(aws_glue_sr_avro_glue_serde):
    assert aws_glue_sr_avro_glue_serde.format() == "avro"

    test_data = {"name": "test_user", "favorite_number": 123, "favorite_color": "blue"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_avro_glue_serde.serializer()
    deserializer = aws_glue_sr_avro_glue_serde.deserializer()

    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    deserialized_data = deserializer(serialized_data, ctx)
    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_avro_glue_deserializer_with_schema_id(
    aws_glue_sr_avro_glue_serde, aws_glue_sr_mock_avro_glue_client
):
    _, schema_version_id = aws_glue_sr_mock_avro_glue_client
    test_data = {"name": "test_user", "favorite_number": 456, "favorite_color": "red"}
    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_avro_glue_serde.serializer()
    deserializer = aws_glue_sr_avro_glue_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == schema_version_id


# Protobuf Tests


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_glue_serializer_deserializer(aws_glue_sr_protobuf_glue_serde):
    assert aws_glue_sr_protobuf_glue_serde.format() == "protobuf"

    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_protobuf_glue_serde.serializer()
    deserializer = aws_glue_sr_protobuf_glue_serde.deserializer()

    serialized_data = serializer(test_data, ctx)
    assert isinstance(serialized_data, bytes)
    assert len(serialized_data) > 0

    deserialized_data = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data


@pytest.mark.unit
@pytest.mark.kafka
def test_protobuf_glue_deserializer_with_schema_id(
    aws_glue_sr_protobuf_glue_serde, aws_glue_sr_mock_protobuf_glue_client
):
    _, schema_version_id = aws_glue_sr_mock_protobuf_glue_client
    test_data = {"name": "John Doe", "id": 123, "email": "john.doe@example.com"}

    ctx = SerializationContext("test_topic", MessageField.VALUE)

    serializer = aws_glue_sr_protobuf_glue_serde.serializer()
    deserializer = aws_glue_sr_protobuf_glue_serde.deserializer_with_schema_id()

    serialized_data = serializer(test_data, ctx)
    deserialized_data, schema_id = deserializer(serialized_data, ctx)

    assert deserialized_data == test_data
    assert schema_id == schema_version_id

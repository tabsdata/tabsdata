"""``build_serde`` dispatcher + ``build_schema_registry_client``
factory."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.kafka import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    Protobuf,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._builder import (
    build_schema_registry_client,
    build_serde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._confluent import (
    AvroSrKafkaSerde,
    JsonSrKafkaSerde,
    ProtobufSrKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._glue import (
    AvroGlueKafkaSerde,
    JsonGlueKafkaSerde,
    ProtobufGlueKafkaSerde,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._no_sr import (
    AvroKafkaSerde,
    JsonKafkaSerde,
    ProtobufKafkaSerde,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.kafka


def _creds() -> UserPassword:
    return UserPassword(user=Secret("u"), password=Secret("p"))


def _aws_creds() -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret("u"),
        secret_access_key=Secret("p"),
    )


_AVRO_SCHEMA = json.dumps({
    "type": "record",
    "name": "X",
    "fields": [{"name": "id", "type": "int"}],
})
_PROTO_SCHEMA = (
    'syntax = "proto3";\n'
    'package example;\n'
    'message Event { int32 id = 1; }\n'
)


# --- build_serde dispatch matrix -----------------------------------


def test_build_serde_no_sr_json(tmp_path):
    serde = build_serde(
        data_format="json", schema="",
        schema_registry=None, working_dir=str(tmp_path),
    )
    assert isinstance(serde, JsonKafkaSerde)


def test_build_serde_no_sr_avro(tmp_path):
    serde = build_serde(
        data_format="avro", schema=_AVRO_SCHEMA,
        schema_registry=None, working_dir=str(tmp_path),
    )
    assert isinstance(serde, AvroKafkaSerde)


def test_build_serde_no_sr_protobuf(tmp_path):
    serde = build_serde(
        data_format=Protobuf(message_name="example.Event"),
        schema=_PROTO_SCHEMA,
        schema_registry=None, working_dir=str(tmp_path),
    )
    assert isinstance(serde, ProtobufKafkaSerde)


def test_build_serde_confluent_json(tmp_path):
    sr = ConfluentSchemaRegistry(url="http://sr")
    serde = build_serde(
        data_format="json", schema="",
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=MagicMock(),
    )
    assert isinstance(serde, JsonSrKafkaSerde)


def test_build_serde_confluent_avro(tmp_path):
    sr = ConfluentSchemaRegistry(url="http://sr")
    serde = build_serde(
        data_format="avro", schema=_AVRO_SCHEMA,
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=MagicMock(),
    )
    assert isinstance(serde, AvroSrKafkaSerde)


def test_build_serde_confluent_protobuf(tmp_path):
    sr = ConfluentSchemaRegistry(url="http://sr")
    serde = build_serde(
        data_format=Protobuf(message_name="example.Event"),
        schema=_PROTO_SCHEMA,
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=MagicMock(),
        sr_client_alt=MagicMock(),
    )
    assert isinstance(serde, ProtobufSrKafkaSerde)


def test_build_serde_glue_json(tmp_path):
    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1", credentials=_aws_creds(),
        registry_name="reg", schema_name="sch",
    )
    glue_client = MagicMock()
    glue_client.exceptions.EntityNotFoundException = type(
        "EntityNotFoundException", (Exception,), {},
    )
    glue_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": "00000000-0000-0000-0000-000000000001",
    }
    serde = build_serde(
        data_format="json", schema=json.dumps({"type": "object"}),
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=glue_client,
    )
    assert isinstance(serde, JsonGlueKafkaSerde)


def test_build_serde_glue_avro(tmp_path):
    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1", credentials=_aws_creds(),
        registry_name="reg", schema_name="sch",
    )
    glue_client = MagicMock()
    glue_client.exceptions.EntityNotFoundException = type(
        "EntityNotFoundException", (Exception,), {},
    )
    glue_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": "00000000-0000-0000-0000-000000000002",
    }
    serde = build_serde(
        data_format="avro", schema=_AVRO_SCHEMA,
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=glue_client,
    )
    assert isinstance(serde, AvroGlueKafkaSerde)


def test_build_serde_glue_protobuf(tmp_path):
    sr = AwsGlueSchemaRegistry(
        region_name="us-east-1", credentials=_aws_creds(),
        registry_name="reg", schema_name="sch",
    )
    glue_client = MagicMock()
    glue_client.exceptions.EntityNotFoundException = type(
        "EntityNotFoundException", (Exception,), {},
    )
    glue_client.get_schema_by_definition.return_value = {
        "SchemaVersionId": "00000000-0000-0000-0000-000000000003",
    }
    serde = build_serde(
        data_format=Protobuf(message_name="example.Event"),
        schema=_PROTO_SCHEMA,
        schema_registry=sr, working_dir=str(tmp_path),
        sr_client=glue_client,
    )
    assert isinstance(serde, ProtobufGlueKafkaSerde)


# --- build_schema_registry_client factory --------------------------


def test_build_sr_client_none_returns_none():
    assert build_schema_registry_client(schema_registry=None) is None


def test_build_sr_client_confluent_no_credentials():
    sr = ConfluentSchemaRegistry(url="http://sr:8081")
    with patch(
        "confluent_kafka.schema_registry.SchemaRegistryClient",
    ) as mock_client:
        client = build_schema_registry_client(schema_registry=sr)
    mock_client.assert_called_once()
    args, _ = mock_client.call_args
    configs = args[0]
    assert configs["url"] == "http://sr:8081"
    assert "basic.auth.user.info" not in configs
    assert client is mock_client.return_value


def test_build_sr_client_confluent_with_credentials():
    sr = ConfluentSchemaRegistry(url="http://sr", credentials=_creds())
    with patch(
        "confluent_kafka.schema_registry.SchemaRegistryClient",
    ) as mock_client:
        build_schema_registry_client(schema_registry=sr)
    configs = mock_client.call_args.args[0]
    assert configs["basic.auth.user.info"] == "u:p"


def test_build_sr_client_glue_populates_aws_envvars_and_calls_boto3():
    import os
    sr = AwsGlueSchemaRegistry(
        region_name="us-west-2", credentials=_aws_creds(),
        registry_name="reg", schema_name="sch",
    )
    saved_id = os.environ.get("AWS_ACCESS_KEY_ID")
    saved_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    try:
        with patch("boto3.client") as mock_boto:
            build_schema_registry_client(schema_registry=sr)
        mock_boto.assert_called_once()
        _service, kwargs = mock_boto.call_args.args, mock_boto.call_args.kwargs
        assert mock_boto.call_args.args[0] == "glue"
        assert kwargs["region_name"] == "us-west-2"
        # AWS credentials env-vars populated from the SR config.
        assert os.environ["AWS_ACCESS_KEY_ID"] == "u"
        assert os.environ["AWS_SECRET_ACCESS_KEY"] == "p"
    finally:
        # Restore to keep the test process clean.
        if saved_id is not None:
            os.environ["AWS_ACCESS_KEY_ID"] = saved_id
        else:
            os.environ.pop("AWS_ACCESS_KEY_ID", None)
        if saved_secret is not None:
            os.environ["AWS_SECRET_ACCESS_KEY"] = saved_secret
        else:
            os.environ.pop("AWS_SECRET_ACCESS_KEY", None)


def test_build_sr_client_glue_forwards_extra_boto_kwargs():
    sr = AwsGlueSchemaRegistry(
        region_name="eu-west-1", credentials=_aws_creds(),
        registry_name="reg", schema_name="sch",
        configs={"endpoint_url": "https://localstack"},
    )
    with patch("boto3.client") as mock_boto:
        build_schema_registry_client(schema_registry=sr)
    kwargs = mock_boto.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://localstack"


def test_build_sr_client_unsupported_type_raises_kafka_10():
    class _BogusSr:
        pass

    with pytest.raises(TabsdataException) as ei:
        build_schema_registry_client(
            schema_registry=_BogusSr(),  # type: ignore[arg-type]
        )
    assert ei.value.api_error is KafkaErrorCode.KAFKA_10

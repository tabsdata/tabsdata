"""Serde dispatcher + schema-registry client factory.

The connector's plugin calls :func:`build_serde` once per session
to pick the right concrete serde for the user's
``(data_format, schema_registry)`` pair, and
:func:`build_schema_registry_client` to construct the Confluent
or AWS Glue client the SR-aware serdes plug into.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from tabsdatak.conn.kafka import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaMessageFormatSpec,
    Protobuf,
    SchemaRegistrySpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes._base import KafkaSerde
from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover
    pass


logger = logging.getLogger(__name__)


def _sr_mode(schema_registry: SchemaRegistrySpec | None) -> str:
    if schema_registry is None:
        return "none"
    if isinstance(schema_registry, ConfluentSchemaRegistry):
        return "confluent"
    if isinstance(schema_registry, AwsGlueSchemaRegistry):
        return "glue"
    return "unknown"


def build_serde(
    *,
    data_format: KafkaMessageFormatSpec,
    schema: str,
    schema_registry: SchemaRegistrySpec | None,
    working_dir: str,
    sr_client: Any | None = None,
    sr_client_alt: Any | None = None,
) -> KafkaSerde:
    """Pick the concrete :class:`KafkaSerde` for the given
    ``(data_format, schema_registry)`` combo.

    :param data_format: ``"avro"`` / ``"json"`` / :class:`Protobuf`.
    :param schema: Schema definition string (Avro JSON, JSON
        Schema, or ``.proto`` source).
    :param schema_registry: ``None`` for the no-SR family,
        :class:`ConfluentSchemaRegistry` for the Confluent
        family, :class:`AwsGlueSchemaRegistry` for the Glue
        family.
    :param working_dir: Scratch dir for protobuf compilation.
    :param sr_client: Pre-built schema-registry client. Used by
        Confluent / Glue paths.
    :param sr_client_alt: Second Confluent SR client (Protobuf SR
        only -- the serializer + deserializer caches diverge and
        need separate clients; see
        :class:`ProtobufSrKafkaSerde`).
    """
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

    mode = _sr_mode(schema_registry)
    is_protobuf = isinstance(data_format, Protobuf)
    fmt_label = "protobuf" if is_protobuf else data_format

    if mode == "none":
        if data_format == "json":
            return JsonKafkaSerde(json_schema=schema or None)
        if data_format == "avro":
            return AvroKafkaSerde(avro_schema=schema)
        if is_protobuf:
            return ProtobufKafkaSerde(
                protobuf_schema=schema,
                message_name=data_format.message_name,
                working_dir=working_dir,
            )

    if mode == "confluent":
        if data_format == "json":
            return JsonSrKafkaSerde(
                schema_registry_client=sr_client,
                json_schema=schema or None,
            )
        if data_format == "avro":
            return AvroSrKafkaSerde(
                schema_registry_client=sr_client,
                avro_schema=schema or None,
            )
        if is_protobuf:
            return ProtobufSrKafkaSerde(
                serializer_registry_client=sr_client,
                deserializer_registry_client=sr_client_alt or sr_client,
                protobuf_schema=schema,
                message_name=data_format.message_name,
                working_dir=working_dir,
            )

    if mode == "glue":
        assert isinstance(schema_registry, AwsGlueSchemaRegistry)
        registry_name = _resolve(schema_registry.registry_name)
        schema_name = _resolve(schema_registry.schema_name)
        if data_format == "json":
            return JsonGlueKafkaSerde(
                glue_client=sr_client,
                registry_name=registry_name,
                schema_name=schema_name,
                json_schema=schema,
            )
        if data_format == "avro":
            return AvroGlueKafkaSerde(
                glue_client=sr_client,
                registry_name=registry_name,
                schema_name=schema_name,
                avro_schema=schema,
            )
        if is_protobuf:
            return ProtobufGlueKafkaSerde(
                glue_client=sr_client,
                registry_name=registry_name,
                schema_name=schema_name,
                protobuf_schema=schema,
                message_name=data_format.message_name,
                working_dir=working_dir,
            )

    raise KafkaErrorCode.KAFKA_9.exception(
        format=fmt_label,
        sr_mode=mode,
    )


def build_schema_registry_client(
    *,
    schema_registry: SchemaRegistrySpec | None,
):
    """Construct the SR client for the given registry config.
    Returns ``None`` when no registry is configured.

    Confluent -> ``SchemaRegistryClient``; Glue -> ``boto3.client('glue')``
    after the AWS access-key envvars are populated from the
    configured credentials.
    """
    if schema_registry is None:
        return None

    if isinstance(schema_registry, ConfluentSchemaRegistry):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        configs: dict[str, Any] = dict(schema_registry.configs or {})
        configs["url"] = _resolve(schema_registry.url)
        if schema_registry.credentials is not None:
            user = schema_registry.credentials.user.value()
            password = schema_registry.credentials.password.value()
            configs["basic.auth.user.info"] = f"{user}:{password}"
        return SchemaRegistryClient(configs)

    if isinstance(schema_registry, AwsGlueSchemaRegistry):
        import boto3

        # Glue boto3 client picks credentials off the environment.
        # We populate the standard envvars from the config.
        os.environ["AWS_ACCESS_KEY_ID"] = schema_registry.credentials.access_key_id.value()
        os.environ["AWS_SECRET_ACCESS_KEY"] = (
            schema_registry.credentials.secret_access_key.value()
        )
        boto_kwargs: dict[str, Any] = {
            "region_name": _resolve(schema_registry.region_name),
        }
        if schema_registry.configs:
            boto_kwargs.update(schema_registry.configs)
        return boto3.client("glue", **boto_kwargs)

    raise KafkaErrorCode.KAFKA_10.exception(
        sr=type(schema_registry).__name__,
    )


def _resolve(spec: Any) -> str:
    """Return the string form of a ``str`` / ``Secret`` / ``Env``
    spec."""
    return spec if isinstance(spec, str) else spec.value()

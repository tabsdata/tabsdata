"""Kafka connector -- public API."""

from __future__ import annotations

from typing import Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.common.types import CfgSpec, UserPassword
from tabsdatak.conn.kafka.error import KafkaErrorCode


# --- Credential aliases ---------------------------------------------


# SASL username + password. Bare strings are auto-wrapped by the
# framework as ``Secret.lit(...)`` at field-validation time.
KafkaCredentialsSpec: TypeAlias = UserPassword

# Confluent SR uses basic-auth (user + password); AWS Glue uses an
# S3-style access-key pair (kept as ``UserPassword`` for shape
# parity -- ``user`` is the access-key-id, ``password`` is the
# secret-access-key).
ConfluentSchemaRegistryCredentialsSpec: TypeAlias = UserPassword
AwsGlueSchemaRegistryCredentialsSpec: TypeAlias = UserPassword


# --- Protobuf message-format marker ---------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_4, frozen=True, kw_only=True)
class Protobuf:
    message_name: str


# Wire-format of the Kafka payload.
KafkaMessageFormatSpec: TypeAlias = Union[Literal["avro", "json"], Protobuf]


# --- Schema registries ----------------------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_2, kw_only=True)
class ConfluentSchemaRegistry:
    url: str
    credentials: ConfluentSchemaRegistryCredentialsSpec | None = None
    configs: CfgSpec | None = None


@_api.dataclass(KafkaErrorCode.KAFKA_3, kw_only=True)
class AwsGlueSchemaRegistry:
    region_name: str
    credentials: AwsGlueSchemaRegistryCredentialsSpec
    registry_name: str
    schema_name: str
    configs: CfgSpec | None = None


# Discriminated union of supported schema-registry types. ``None``
# (the default on :class:`KafkaConn`) means schema-less ingest.
SchemaRegistrySpec: TypeAlias = Union[
    ConfluentSchemaRegistry, AwsGlueSchemaRegistry,
]

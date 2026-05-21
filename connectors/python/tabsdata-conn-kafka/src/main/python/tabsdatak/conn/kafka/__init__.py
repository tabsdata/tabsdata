"""Kafka connector -- public API."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import Conn, NamedConn, StreamSrc

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import CfgSpec, EntityWhitelistSpec, UserPassword
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


# --- Bootstrap-servers + topic aliases ------------------------------


# A single bootstrap server (host[:port]) -- the format
# ``confluent_kafka`` expects.
KafkaServerSpec: TypeAlias = str

# Bootstrap-server list. Confluent-kafka also accepts a single
# comma-separated string; both shapes are honoured.
KafkaServersSpec: TypeAlias = Union[KafkaServerSpec, list[KafkaServerSpec]]

# Topic name -- Kafka topics permit a wide alphabet, validation is
# left to the broker on subscribe.
KafkaTopicSpec: TypeAlias = str


# --- KafkaConn ------------------------------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_1, kw_only=True)
class KafkaConn(Conn):
    servers: KafkaServersSpec
    credentials: KafkaCredentialsSpec | None = None
    group_id: str | None = None
    schema_registry: SchemaRegistrySpec | None = None
    enforce_connection_params: bool = True
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None


ConnSpec: TypeAlias = Union[NamedConn, KafkaConn]


# --- KafkaStreamSrc.src_cfg keys ------------------------------------
#
# Mirrors the CDC connectors' ``src_cfg`` shape. The rollover knobs
# control when the plugin closes one parquet batch and stages it.

SrcCfgKey: TypeAlias = Literal[
    "kafka.consume_timeout_seconds",
    "kafka.connection_timeout_seconds",
    "kafka.logging_level",
    "kafka.time_rollover_seconds",
    "kafka.size_rollover_mb",
    "kafka.messages_rollover",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


@_api.dataclass(KafkaErrorCode.KAFKA_5, kw_only=True)
class KafkaStreamSrc(StreamSrc):
    conn: ConnSpec
    topic: KafkaTopicSpec
    data_format: KafkaMessageFormatSpec
    schema: str
    group_id: str | None = None
    table: str | None = None
    dlq_topic: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    src_cfg: SrcCfgSpec | None = None

    def output_table(self) -> str:
        return self.table or self.topic

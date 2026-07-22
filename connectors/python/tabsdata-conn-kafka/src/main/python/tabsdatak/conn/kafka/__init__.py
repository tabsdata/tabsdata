#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka connector -- public API.

Streaming only (no batch ``Src`` / ``Dest``). Modelled on the CDC
connectors (mysql / postgres / db2):

* :class:`KafkaStreamSrcConn` -- per-instance Kafka connection: bootstrap
  servers, SASL credentials, consumer group id, optional schema
  registry.
* :class:`KafkaStreamSrc` -- stream source that pulls from a single
  Kafka ``topic`` and emits one tableframe-batch per rollover
  (time / size / message-count thresholds).
* :class:`ConfluentSchemaRegistry` / :class:`AwsGlueSchemaRegistry`
  -- optional schema-registry configurations attached to
  :class:`KafkaStreamSrcConn`.
* :class:`Protobuf` -- :data:`KafkaMessageFormatSpec` variant
  carrying the protobuf message type name.

Bootstrap servers, SASL credentials, schema-registry URL /
region / names accept ``Secret`` references. CDC-style
cfg keys live in ``KafkaStreamSrc.src_cfg`` (rollover thresholds,
logging level).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

from pydantic import StringConstraints

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import StreamSrc, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    CfgSpec,
    StrOrSecretConstraints,
    UserPassword,
    _convert,
)
from tabsdatak.conn.kafka.error import KafkaErrorCode
from tabsdatak.spi import Conn

# --- Credential aliases ---------------------------------------------


# SASL username + password. Bare strings are auto-wrapped by the
# framework as ``Secret(...)`` at field-validation time.
KafkaCredentialsSpec: TypeAlias = UserPassword

# Confluent SR uses basic-auth (user + password); AWS Glue uses an
# S3-style access-key pair (``access_key_id`` + ``secret_access_key``).
ConfluentSchemaRegistryCredentialsSpec: TypeAlias = UserPassword
AwsGlueSchemaRegistryCredentialsSpec: TypeAlias = AwsAccessSecretKey


# --- Protobuf message-format marker ---------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_4, frozen=True, kw_only=True)
class Protobuf:
    """Marker carrying the Protobuf message type name. Used as a
    :data:`KafkaMessageFormatSpec` variant when ingesting Protobuf
    payloads.

    :param message_name: Fully-qualified protobuf message name
        (e.g. ``"com.example.UserEvent"``) -- the descriptor
        lookup key used at deserialisation time.
    """

    message_name: str


# Wire-format of the Kafka payload.
KafkaMessageFormatSpec: TypeAlias = Union[Literal["avro", "json"], Protobuf]


# --- Schema registries ----------------------------------------------

_CONFLUENT_SCHEMA_REGISTRY_URL_NAME_RE_STR = r".*"
ConfluentSchemaRegistryUrlNameSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    StrOrSecretConstraints(pattern=_CONFLUENT_SCHEMA_REGISTRY_URL_NAME_RE_STR),
]


@_api.dataclass(KafkaErrorCode.KAFKA_2, kw_only=True)
class ConfluentSchemaRegistry:
    """Confluent-style schema registry attached to a
    ``KafkaStreamSrcConn``.

    :param url: Registry HTTP(S) URL.
    :param credentials: Optional basic-auth credentials (resolved
        at session open via the framework's ``Secret`` machinery).
    :param configs: Optional dict of registry-client overrides
        forwarded to
        ``confluent_kafka.schema_registry.SchemaRegistryClient``.
    """

    url: ConfluentSchemaRegistryUrlNameSpec
    credentials: ConfluentSchemaRegistryCredentialsSpec | None = None
    configs: CfgSpec | None = None

    def _url(self) -> str:
        return _convert(self.url, str)


_REGISTRY_NAME_RE_STR = r".*"
RegistryNameSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_REGISTRY_NAME_RE_STR)
]

_SCHEMA_NAME_RE_STR = r".*"
SchemaNameSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_SCHEMA_NAME_RE_STR)
]


@_api.dataclass(KafkaErrorCode.KAFKA_3, kw_only=True)
class AwsGlueSchemaRegistry:
    """AWS Glue schema registry attached to a ``KafkaStreamSrcConn``.

    :param region_name: AWS region the registry lives in
        (e.g. ``"us-east-1"``).
    :param credentials: :class:`AwsAccessSecretKey` with the
        ``access_key_id`` / ``secret_access_key`` pair Glue
        authenticates against.
    :param registry_name: Glue registry name.
    :param schema_name: Schema name within the registry.
    :param configs: Optional dict of boto3-client overrides.
    """

    region_name: AwsRegionSpec
    credentials: AwsGlueSchemaRegistryCredentialsSpec
    registry_name: RegistryNameSpec
    schema_name: SchemaNameSpec
    configs: CfgSpec | None = None

    def _region_name(self) -> str:
        return _convert(self.region_name, str)

    def _registry_name(self) -> str:
        return _convert(self.registry_name, str)

    def _schema_name(self) -> str:
        return _convert(self.schema_name, str)


# Discriminated union of supported schema-registry types. ``None``
# (the default on :class:`KafkaStreamSrcConn`) means schema-less ingest.
SchemaRegistrySpec: TypeAlias = Union[
    ConfluentSchemaRegistry,
    AwsGlueSchemaRegistry,
]


# --- Bootstrap-servers + topic aliases ------------------------------


# Bootstrap-server list. A single comma-separated string of (host[:port]).
_SERVERS_VALUE_PATTERN = r".+"
KafkaServersSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_SERVERS_VALUE_PATTERN)
]

# Topic name -- Kafka topics permit a wide alphabet, validation is
# left to the broker on subscribe.
_GROUP_VALUE_PATTERN = r".+"
KafkaGroupSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_GROUP_VALUE_PATTERN)
]

# Schema
_SCHEMA_PATTERN = r".+"
SchemaSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_SCHEMA_PATTERN)],
]

# Topic name -- Kafka topics permit a wide alphabet, validation is
# left to the broker on subscribe.
_TOPIC_PATTERN = r".+"
KafkaTopicSpec: TypeAlias = Union[
    Annotated[str, StringConstraints(pattern=_TOPIC_PATTERN)],
]


# --- KafkaStreamSrcConn ------------------------------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_1, kw_only=True)
class KafkaStreamSrcConn(Conn):
    """Kafka connection.

    :param servers: Bootstrap-server list -- a single
        comma-separated string of ``host[:port]`` items, or an
        ``Secret`` ref that resolves to one.
    :param credentials: :class:`UserPassword` for SASL/SCRAM-256
        auth. Omit for an unauthenticated cluster.
    :param group_id: Consumer-group id used by the source's
        polling consumer. Required.
    :param schema_registry: Optional :class:`ConfluentSchemaRegistry`
        or :class:`AwsGlueSchemaRegistry` for SR-aware serdes.
    :param conn_cfg: Optional driver-side config (forwarded as
        consumer ``configs`` to ``confluent_kafka.Consumer``).
    """

    servers: KafkaServersSpec
    credentials: KafkaCredentialsSpec | None = None
    group_id: KafkaGroupSpec
    schema_registry: SchemaRegistrySpec | None = None
    conn_cfg: CfgSpec | None = None

    def _servers(self) -> str:
        return _convert(self.servers, str)

    def _group_id(self) -> str:
        return _convert(self.group_id, str)


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
    """Kafka stream source -- consumes a single topic and emits one
    tableframe-batch per rollover.

    The legacy ``KafkaTrigger`` ran outside the manifest-stage
    contract; this stream source plugs into the same
    :class:`StreamSrcPluginCtx.stage` / ``commit`` flow the CDC
    sources use. Each rollover yields a single-table positional
    manifest pointing at the parquet file the buffer flushed to.

    :param data_format: Wire format -- ``"avro"`` / ``"json"`` or
        :class:`Protobuf`.
    :param schema: Schema definition string for the data format
        (an Avro schema, a JSON Schema, or a Protobuf ``.proto``).
        Required for non-SR mode; SR-aware modes still use this to
        cross-check the registry's resolved schema.
    :param topic: Kafka topic to consume from. The output table
        name on the flow side is the topic name.
    :param dlq_topic: Optional dead-letter topic that
        deserialisation failures are produced to.
    :param src_cfg: Optional ``Mapping`` over :data:`SrcCfgKey` --
        rollover thresholds, logging level, connection /
        consume timeouts.
    """

    data_format: KafkaMessageFormatSpec
    schema: str
    topic: KafkaTopicSpec
    dlq_topic: KafkaTopicSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def output_table(self) -> str:
        """Resolve the output table name -- the source's ``topic``."""
        return self.topic

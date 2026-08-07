#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka connector -- public API.

Streaming source only (no batch `Src` and no `Dest`); modelled on
the CDC connectors (mysql / postgres / db2). Imported from
`tabsdatak.conn.kafka`:

* `KafkaStreamSrcConn` -- the connection: bootstrap `servers`,
  optional SASL `credentials`, consumer `group_id` and an optional
  `schema_registry`. Registered on a collection, not passed to a
  decorator.
* `KafkaStreamSrc` -- the `@stream_publisher` source: pulls from a
  single `topic` and emits one tableframe batch per rollover
  (time / size / message-count threshold).
* `ConfluentSchemaRegistry` / `AwsGlueSchemaRegistry` -- optional
  schema-registry configs set as the connection's `schema_registry`.
* `Protobuf` -- a `data_format` variant carrying the protobuf
  message type name (the other formats are the strings `"avro"` /
  `"json"`).

The connection (with credentials) is resolved from the function's
collection at run time; a flow function only references
`KafkaStreamSrc`. A `@stream_publisher` function receives the
flushed batch as a list of frames and must NOT declare a `ctx`
parameter.

Examples:
    .. code-block:: python

        @stream_publisher(
            source=KafkaStreamSrc(data_format="json", schema=SCHEMA,
                                  topic="events"),
            flush_interval_mins=2,
            output_tables=["events"],
        )
        def ingest(events: TableFramesSpec) -> TableFrameSpec:
            return concat(events)
Bootstrap servers, SASL credentials and schema-registry URL / region /
names accept a plain string or a `Secret`. Rollover thresholds and
the logging level live in `KafkaStreamSrc.src_cfg`.
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
# framework as `Secret(...)` at field-validation time.
KafkaCredentialsSpec: TypeAlias = UserPassword

# Confluent SR uses basic-auth (user + password); AWS Glue uses an
# S3-style access-key pair (`access_key_id` + `secret_access_key`).
ConfluentSchemaRegistryCredentialsSpec: TypeAlias = UserPassword
AwsGlueSchemaRegistryCredentialsSpec: TypeAlias = AwsAccessSecretKey


# --- Protobuf message-format marker ---------------------------------


@_api.dataclass(KafkaErrorCode.KAFKA_4, frozen=True, kw_only=True)
class Protobuf:
    """Protobuf payload marker carrying the message type name.

    Used as the `data_format` of a `KafkaStreamSrc` when ingesting
    Protobuf payloads (the `"avro"` / `"json"` string variants
    cover the other wire formats).

    Args:
        message_name: Fully-qualified protobuf message name (e.g.
            `"com.example.UserEvent"`) -- the descriptor lookup key
            used at deserialization time.
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
    `KafkaStreamSrcConn`.

    :categories: source

    Set as the connection's `schema_registry` for SR-aware serdes.

    Args:
        url: Registry HTTP(S) URL; accepts a plain string or a
            `Secret`.
        credentials: Optional `UserPassword` basic-auth credentials.
        configs: Optional dict of registry-client overrides forwarded
            to `confluent_kafka.schema_registry.SchemaRegistryClient`
            (see `CfgSpec`).
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
    """AWS Glue schema registry attached to a `KafkaStreamSrcConn`.

    :categories: source

    Set as the connection's `schema_registry` for SR-aware serdes.

    Args:
        region_name: AWS region the registry lives in, e.g.
            `us-east-1`; accepts a plain string or a `Secret`.
        credentials: `AwsAccessSecretKey` (access key id / secret
            access key) Glue authenticates against.
        registry_name: Glue registry name; plain string or `Secret`.
        schema_name: Schema name within the registry; plain string or
            `Secret`.
        configs: Optional dict of boto3-client overrides (see
            `CfgSpec`).
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


# Discriminated union of supported schema-registry types. `None`
# (the default on `KafkaStreamSrcConn`) means schema-less ingest.
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
    """Kafka connection used by `KafkaStreamSrc`.

    :categories: source

    Registered on a collection; the runtime resolves it and hands it to
    the source at run time. `servers` and `group_id` accept a plain
    string or a `Secret`.

    Args:
        servers: Bootstrap-server list as a single comma-separated
            string of `host[:port]` items (any non-empty value), or a
            `Secret` that resolves to one.
        credentials: Optional `UserPassword` for SASL/SCRAM auth;
            omit for an unauthenticated cluster.
        group_id: Consumer-group id used by the polling consumer (any
            non-empty value). Required.
        schema_registry: Optional `ConfluentSchemaRegistry` or
            `AwsGlueSchemaRegistry`; omit for schema-less ingest.
        conn_cfg: Optional connector config forwarded as consumer
            `configs` to `confluent_kafka.Consumer` (see
            `CfgSpec`).

    Examples:
        A connection with a Confluent schema registry for SR-aware
        serdes:

        .. code-block:: python

            KafkaStreamSrcConn(
                servers="broker:9092",
                group_id="ingest",
                schema_registry=ConfluentSchemaRegistry(
                    url="http://registry:8081",
                ),
            )
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
# Mirrors the CDC connectors' `src_cfg` shape. The rollover knobs
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
    """Kafka streaming source for `@stream_publisher` -- one batch
    per rollover.

    :categories: source

    Consumes a single Kafka `topic` and emits one tableframe batch
    each time a rollover threshold fires (time / size / message count).
    Used as the `source` of a `@stream_publisher`; that function
    receives the batch as a list of frames and must NOT declare a
    `ctx` parameter. The emitted table name is the `topic`.

    Every batch this source stages automatically carries a
    `@td.kafka.topic` column holding the topic the message was consumed
    from.

    Args:
        data_format: Payload wire format -- `"avro"`, `"json"` or a
            `Protobuf(message_name=...)` marker.
        schema: Schema definition string for `data_format` (an Avro
            schema, a JSON Schema, or a Protobuf `.proto`). Required;
            in schema-registry mode it also cross-checks the registry's
            resolved schema.
        topic: Kafka topic to consume; also the emitted table name.
        dlq_topic: Optional dead-letter topic that deserialization
            failures are produced to.
        src_cfg: Optional connector config over the keys
            `kafka.consume_timeout_seconds`,
            `kafka.connection_timeout_seconds`,
            `kafka.logging_level`, `kafka.time_rollover_seconds`,
            `kafka.size_rollover_mb` and `kafka.messages_rollover`.

    Examples:
        Consume Avro payloads, staging a batch every 500 messages
        via a `src_cfg` rollover threshold:

        .. code-block:: python

            @stream_publisher(
                source=KafkaStreamSrc(
                    topic="users",
                    data_format="avro",
                    schema=AVRO_SCHEMA,
                    src_cfg={"kafka.messages_rollover": 500},
                ),
                flush_interval_mins=1,
                output_tables=["users"],
            )
            def ingest(users: TableFramesSpec) -> TableFrameSpec:
                return concat(users)

        Consume Protobuf payloads; the `Protobuf` marker carries the
        message type name:

        .. code-block:: python

            @stream_publisher(
                source=KafkaStreamSrc(
                    topic="persons",
                    data_format=Protobuf(message_name="PersonV1"),
                    schema=PROTO_SCHEMA,
                ),
                flush_interval_mins=1,
                output_tables=["persons"],
            )
            def ingest(persons: TableFramesSpec) -> TableFrameSpec:
                return concat(persons)
    """

    data_format: KafkaMessageFormatSpec
    schema: str
    topic: KafkaTopicSpec
    dlq_topic: KafkaTopicSpec | None = None
    src_cfg: SrcCfgSpec | None = None

    def output_table(self) -> str:
        """Return the emitted table name -- the source's `topic`."""
        return self.topic

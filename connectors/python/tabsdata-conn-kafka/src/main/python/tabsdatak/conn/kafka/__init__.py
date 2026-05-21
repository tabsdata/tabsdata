"""Kafka connector -- public API.

Streaming only (no batch ``Src`` / ``Dest``). Modelled on the CDC
connectors (mysql / postgres / db2):

* :class:`KafkaConn` -- per-instance Kafka connection: bootstrap
  servers, SASL credentials, optional consumer group, optional
  schema registry.
* :class:`KafkaStreamSrc` -- stream source that pulls from a single
  Kafka ``topic`` and emits one tableframe-batch per rollover
  (time / size / message-count thresholds).
* :class:`ConfluentSchemaRegistry` / :class:`AwsGlueSchemaRegistry`
  -- optional schema-registry configurations attached to
  :class:`KafkaConn`.
* :class:`Protobuf` -- :data:`KafkaMessageFormatSpec` variant
  carrying the protobuf message type name.

Bootstrap-server lists and SASL credentials accept ``Env`` /
``Secret`` references. CDC-style cfg keys live in
``KafkaStreamSrc.src_cfg`` (rollover thresholds, logging level).
"""

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


@_api.dataclass(KafkaErrorCode.KAFKA_2, kw_only=True)
class ConfluentSchemaRegistry:
    """Confluent-style schema registry attached to a
    ``KafkaConn``.

    :param url: Registry HTTP(S) URL.
    :param credentials: Optional basic-auth credentials (resolved
        at session open via the framework's ``Secret`` machinery).
    :param configs: Optional dict of registry-client overrides
        forwarded to
        ``confluent_kafka.schema_registry.SchemaRegistryClient``.
    """

    url: str
    credentials: ConfluentSchemaRegistryCredentialsSpec | None = None
    configs: CfgSpec | None = None


@_api.dataclass(KafkaErrorCode.KAFKA_3, kw_only=True)
class AwsGlueSchemaRegistry:
    """AWS Glue schema registry attached to a ``KafkaConn``.

    :param region_name: AWS region the registry lives in
        (e.g. ``"us-east-1"``).
    :param credentials: AWS access-key credentials -- ``user`` is
        the access-key-id, ``password`` is the secret-access-key.
    :param registry_name: Glue registry name.
    :param schema_name: Schema name within the registry.
    :param configs: Optional dict of boto3-client overrides.
    """

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
    """Kafka connection.

    :param servers: Bootstrap-server list (``host[:port]`` items).
        Comma-separated strings are also accepted.
    :param credentials: :class:`UserPassword` for SASL/SCRAM-256
        auth. Omit for an unauthenticated cluster.
    :param group_id: Default consumer-group id. Connector-level
        default; :class:`KafkaStreamSrc.group_id` overrides per
        source.
    :param schema_registry: Optional :class:`ConfluentSchemaRegistry`
        or :class:`AwsGlueSchemaRegistry` for SR-aware serdes.
    :param enforce_connection_params: When ``True`` (default), the
        connection's ``group_id`` wins over the source's; when
        ``False``, the source's ``group_id`` overrides the
        connection's.
    :param entity_whitelist: Optional list of regex patterns; the
        runtime uses these to narrow which topics this conn
        contributes to discovery.
    :param conn_cfg: Optional driver-side config (forwarded as
        consumer ``configs`` to ``confluent_kafka.Consumer``).
    """

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
    """Kafka stream source -- consumes a single topic and emits one
    tableframe-batch per rollover.

    The legacy ``KafkaTrigger`` ran outside the manifest-stage
    contract; this stream source plugs into the same
    :class:`StreamSrcPluginCtx.stage` / ``commit`` flow the CDC
    sources use. Each rollover yields one :class:`TableManifest`
    pointing at the parquet file the buffer flushed to.

    :param conn: :class:`KafkaConn` (or :class:`NamedConn`).
    :param topic: Kafka topic to consume from. The output-table
        name on the flow side defaults to the topic name unless
        ``table`` is overridden.
    :param data_format: Wire format -- ``"avro"`` / ``"json"`` or
        :class:`Protobuf`.
    :param schema: Schema definition string for the data format
        (an Avro schema, a JSON Schema, or a Protobuf ``.proto``).
        Required for non-SR mode; SR-aware modes still use this to
        cross-check the registry's resolved schema.
    :param group_id: Per-source consumer-group id. If the
        :class:`KafkaConn` has ``enforce_connection_params=True``
        (default) and declares its own ``group_id``, the
        connection wins; otherwise this one is used.
    :param table: Output table name. Defaults to ``topic`` when
        unset.
    :param dlq_topic: Optional dead-letter topic that
        deserialisation failures are produced to.
    :param start: Optional UTC datetime gate: the consumer
        ignores messages until ``start``.
    :param end: Optional UTC datetime gate: the consumer exits
        once ``end`` is reached.
    :param src_cfg: Optional ``Mapping`` over :data:`SrcCfgKey` --
        rollover thresholds, logging level, connection /
        consume timeouts.
    """

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
        """Resolve the output table name: explicit ``table`` if
        set, otherwise the source's ``topic``."""
        return self.table or self.topic

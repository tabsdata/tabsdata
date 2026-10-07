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
  `topic` (every topic matching it when it starts with `^`), writes
  the messages to parquet files as its buffer fills, and emits them as
  one tableframe batch whenever a trigger threshold (time / size /
  message count) is due.
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

Delivery is at least once. The consumer group's offsets are
acknowledged on the broker only once a batch is staged; a partition a
rebalance takes away is staged and acknowledged before it is handed
over; and a consumer that loses its partitions restarts by itself from
the last acknowledged offset, dropping what it had not staged. A
message that cannot be decoded, that its schema refuses, or whose
values do not fit the types of its table goes to the source's
`dlq_topic` when one is set and stops the stream otherwise; a broker
error is never a dead letter.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias, Union

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
import tabsdatak.conn.kafka._validation as _validation
from tabsdatak.api import StreamSrc, StrOrSecretSpec
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.conn.common._stream import _floor, _validation as _stream_validation
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    CfgSpec,
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

# Unvalidated: the registry rejects a URL it cannot serve, and no local
# shape check adds anything.
ConfluentSchemaRegistryUrlNameSpec: TypeAlias = StrOrSecretSpec


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


# Unvalidated: registry naming is the registry's business.
RegistryNameSpec: TypeAlias = StrOrSecretSpec

# Unvalidated: subject naming is the registry's business.
SchemaNameSpec: TypeAlias = StrOrSecretSpec


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
KafkaServersSpec: TypeAlias = Annotated[StrOrSecretSpec, _validation.SERVERS_VALIDATION]

KafkaGroupSpec: TypeAlias = Annotated[StrOrSecretSpec, _validation.GROUP_VALIDATION]

SchemaSpec: TypeAlias = Annotated[str, _validation.SCHEMA_VALIDATION]

KafkaTopicSpec: TypeAlias = Annotated[str, _validation.TOPIC_VALIDATION]


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
            `CfgSpec`). The connector sets `auto.offset.reset=earliest`
            and `enable.auto.commit=false` itself, and defaults
            `partition.assignment.strategy=cooperative-sticky`,
            `topic.metadata.refresh.interval.ms=30000`,
            `fetch.queue.backoff.ms=0` and
            `socket.keepalive.enable=true`; a value given here wins
            over each default. Settings worth knowing:
            `max.poll.interval.ms` (raise it when a stage takes
            longer than its 5 minute default; the stream warns past
            half of it and restarts itself if evicted),
            `queued.max.messages.kbytes` (memory the fetch queue may
            hold), `fetch.min.bytes` / `fetch.wait.max.ms` (latency
            against throughput) and `fetch.message.max.bytes`.

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
# Mirrors the CDC connectors' `src_cfg` shape. The `buffer` knobs
# control when the buffered messages are written to a parquet file;
# the `trigger` knobs control when the written files are staged as
# one batch.

SrcCfgKey: TypeAlias = Literal[
    "tabsdata.kafka.poll.max_secs",
    "kafka.connect_timeout_secs",
    "kafka.logging_level",
    "tabsdata.kafka.buffer.max_rows",
    "tabsdata.kafka.buffer.max_bytes",
    "tabsdata.kafka.buffer.max_secs",
    "tabsdata.kafka.trigger.max_rows",
    "tabsdata.kafka.trigger.max_bytes",
    "tabsdata.kafka.trigger.max_secs",
    "tabsdata.kafka.manifest.min_secs",
    "tabsdata.kafka.manifest.max_secs",
    "tabsdata.kafka.consume.max_messages",
]

SrcCfgSpec: TypeAlias = Annotated[
    Mapping[SrcCfgKey, Any],
    _common_validation.CONN_CFG_VALIDATION,
    _floor.seconds_floor_validator("tabsdata.kafka.trigger.max_secs"),
    _floor.manifest_bounds_validator(
        "tabsdata.kafka.manifest.min_secs",
        "tabsdata.kafka.manifest.max_secs",
        "tabsdata.kafka.trigger.max_secs",
    ),
    _stream_validation.positive_integers_validator(
        "tabsdata.kafka.buffer.max_rows",
        "tabsdata.kafka.buffer.max_bytes",
        "tabsdata.kafka.buffer.max_secs",
        "tabsdata.kafka.trigger.max_rows",
        "tabsdata.kafka.trigger.max_bytes",
        "tabsdata.kafka.poll.max_secs",
        "kafka.connect_timeout_secs",
        "tabsdata.kafka.consume.max_messages",
    ),
]


@_api.dataclass(KafkaErrorCode.KAFKA_5, kw_only=True)
class KafkaStreamSrc(StreamSrc):
    """Kafka streaming source for `@stream_publisher` -- one batch
    per due trigger.

    :categories: source

    Consumes a Kafka `topic` (every topic matching it when it starts
    with `^`) in two levels. The `buffer` keys
    decide when the buffered messages are written to a parquet file;
    the `trigger` keys decide when the written files are staged as one
    tableframe batch. A batch is due once
    `tabsdata.kafka.trigger.max_secs` (or the decorator's
    `flush_interval_mins`, when shorter) have passed since the last one,
    or earlier when `tabsdata.kafka.trigger.max_rows` or
    `tabsdata.kafka.trigger.max_bytes` is reached -- but never sooner
    than `tabsdata.kafka.manifest.min_secs` after the previous batch (60
    by default, never less). `tabsdata.kafka.manifest.max_secs`, unset by
    default, stages an empty batch once that long has passed without one,
    so the publisher runs with empty inputs as a heartbeat. Messages of incompatible
    schemas land in separate files of the same batch; the files of one
    schema keep the column types of the run's earlier files of that
    schema, and a row whose values do not fit them is a dead letter.

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
        topic: Kafka topic to consume -- a regular expression over the
            topic names when it starts with `^`; also the emitted table
            name.
        dlq_topic: Optional dead-letter topic for the messages the source
            cannot stage: the ones that fail to decode, that the schema
            refuses, or whose values do not fit the types of their table
            (the envelope then carries the decoded `record`). Without it
            such a message stops the stream. A broker error is never a
            dead letter.
        src_cfg: Optional connector config over the keys
            `tabsdata.kafka.poll.max_secs` (how long one poll waits for
            messages, default 2), `kafka.connect_timeout_secs` (default
            16), `kafka.logging_level`, the buffer limits
            `tabsdata.kafka.buffer.max_rows` (default 10000),
            `tabsdata.kafka.buffer.max_bytes` (memory the decoded records
            hold, off by default) and `tabsdata.kafka.buffer.max_secs`
            (default 60), and the trigger limits
            `tabsdata.kafka.trigger.max_secs` (default 60, at least 60),
            `tabsdata.kafka.trigger.max_rows` and
            `tabsdata.kafka.trigger.max_bytes` (both off by default), and
            the manifest bounds `tabsdata.kafka.manifest.min_secs`
            (default 60, at least 60, `None` for the default;
            `trigger.max_secs` may not be below it) and
            `tabsdata.kafka.manifest.max_secs` (off by default; at least
            `min_secs` when set).
            Every limit is a positive whole number. A byte limit is
            measured on every record and slows ingestion down; bounding
            by rows is the cheap option and the preferred one.
            `tabsdata.kafka.consume.max_messages` (default 10000) is how
            many messages one fetch from the consumer hands over at once;
            a JSON schema of plain typed properties lets such a fetch be
            decoded as one Arrow table, so a backlog reads faster with a
            larger fetch, up to what librdkafka prefetches into its local
            queue (`queued.min.messages`, 100000 by default, within
            `queued.max.messages.kbytes`, 64 MiB by default); a fetch
            asking for more waits out the poll.

    Examples:
        Consume Avro payloads, staging a batch as soon as 500 messages
        are waiting, and no sooner than a minute after the last one:

        .. code-block:: python

            @stream_publisher(
                source=KafkaStreamSrc(
                    topic="users",
                    data_format="avro",
                    schema=AVRO_SCHEMA,
                    src_cfg={"tabsdata.kafka.trigger.max_rows": 500},
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

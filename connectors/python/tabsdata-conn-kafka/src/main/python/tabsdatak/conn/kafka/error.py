#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common.error import (
    ConnException,
    ConnInitException,
)
from tabsdatak.error import (
    ErrorCode,
    ErrorDef,
)


class KafkaValidateException(ConnException):
    """Cross-field validation failure for the Kafka connector.

    :categories: error
    """


class KafkaRuntimeException(ConnException):
    """Runtime failure (plugin / consumer / serde) for the Kafka
    connector.

    :categories: error
    """


class KafkaErrorCode(ErrorCode):
    """Error codes for the Kafka connector.

    :categories: error

    One variant per validation / runtime site. Raise via
    `KafkaErrorCode.KAFKA_N.exception`.
    """

    # --- Public types ----------------------------------------------

    KAFKA_1 = ErrorDef(
        ConnInitException,
        "KafkaStreamSrcConn validation failed",
    )  # KafkaStreamSrcConn
    KAFKA_2 = ErrorDef(
        ConnInitException,
        "ConfluentSchemaRegistry validation failed",
    )  # ConfluentSchemaRegistry
    KAFKA_3 = ErrorDef(
        ConnInitException,
        "AwsGlueSchemaRegistry validation failed",
    )  # AwsGlueSchemaRegistry
    KAFKA_4 = ErrorDef(
        ConnInitException,
        "Cannot create Protobuf",
    )  # Protobuf message-format dataclass
    KAFKA_5 = ErrorDef(
        ConnInitException,
        "KafkaStreamSrc validation failed",
    )  # KafkaStreamSrc

    # --- Runtime ----------------------------------------------------

    KAFKA_6 = ErrorDef(
        KafkaRuntimeException,
        "Kafka broker connection failed (servers={servers!r})",
    )  # consumer startup: cannot list_topics
    KAFKA_7 = ErrorDef(
        KafkaRuntimeException,
        "Kafka DLQ producer connection failed (servers={servers!r})",
    )  # dlq producer startup
    KAFKA_8 = ErrorDef(
        KafkaRuntimeException,
        "Kafka consumer group.id is required but not set (group_id={group_id!r})",
    )  # _build_kafka_configs: no group.id
    KAFKA_9 = ErrorDef(
        KafkaRuntimeException,
        "Unsupported Kafka message format {format!r} for "
        "schema-registry mode {sr_mode!r}",
    )  # _build_serde: unknown format / sr combination
    KAFKA_10 = ErrorDef(
        KafkaRuntimeException,
        "Unsupported Kafka schema-registry type {sr!r}",
    )  # build_schema_registry_client: not Confluent / Glue
    KAFKA_11 = ErrorDef(
        KafkaRuntimeException,
        "Kafka deserialization failed (topic={topic!r}, offset={offset!r})",
    )  # _process_message: deserialize raised
    KAFKA_12 = ErrorDef(
        KafkaRuntimeException,
        "Kafka end-time reached before any data was consumed (end={end!r})",
    )  # consume_loop: hit `end` empty-handed

    # --- Serdes -----------------------------------------------------

    KAFKA_13 = ErrorDef(
        KafkaRuntimeException,
        "Confluent SR payload too short to carry a schema id "
        "(got {length!r} bytes; expected at least 5)",
    )  # SrKafkaSerde.extract_schema_id
    KAFKA_14 = ErrorDef(
        KafkaRuntimeException,
        "Kafka JSON serde: malformed schema string ({error!s})",
    )  # JsonKafkaSerde / JsonSrKafkaSerde: invalid JSON schema string
    KAFKA_15 = ErrorDef(
        KafkaRuntimeException,
        "Kafka JSON serde: message does not conform to schema ({error!s})",
    )  # JsonKafkaSerde: jsonschema validation failure
    KAFKA_16 = ErrorDef(
        KafkaRuntimeException,
        "Kafka Protobuf serde: cannot compile schema ({error!s})",
    )  # get_protobuf_class: protoc invocation failed
    KAFKA_17 = ErrorDef(
        KafkaRuntimeException,
        "Kafka Protobuf serde: message {message_name!r} not found in compiled schema",
    )  # get_protobuf_class: message_name absent from FileDescriptorSet
    KAFKA_18 = ErrorDef(
        KafkaRuntimeException,
        "AWS Glue schema registry: cannot resolve schema "
        "(registry={registry_name!r}, schema={schema_name!r})",
    )  # AwsGlueDeserializer: get_schema_version failed

    # --- Explorer ---------------------------------------------------

    KAFKA_19 = ErrorDef(
        ConnInitException,
        "KafkaTopicEntity validation failed",
    )  # KafkaTopicEntity
    KAFKA_20 = ErrorDef(
        ConnInitException,
        "KafkaConnMetaModel validation failed",
    )  # KafkaConnMetaModel

    # --- Field-value rules (tabsdatak.conn.kafka) -------------------
    # Kafka's alphabets are broad and the broker is the real authority, so
    # these fields are gated only on being non-empty. The registry-facing
    # specs carry no rule at all (see the aliases in `__init__`).
    KAFKA_21 = ErrorDef(
        ConnInitException, "Kafka bootstrap servers must be non-empty"
    )  # KafkaServersSpec
    KAFKA_22 = ErrorDef(
        ConnInitException, "Kafka consumer group must be non-empty"
    )  # KafkaGroupSpec
    KAFKA_23 = ErrorDef(ConnInitException, "schema must be non-empty")  # SchemaSpec
    KAFKA_24 = ErrorDef(
        ConnInitException, "Kafka topic must be non-empty"
    )  # KafkaTopicSpec

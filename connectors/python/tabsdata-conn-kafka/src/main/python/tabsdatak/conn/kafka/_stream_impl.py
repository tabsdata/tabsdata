#
# Copyright 2026 Tabsdata Inc.
#

"""Kafka stream-source ingester + encoder.

The wire-format (de)serialisers live in the
:mod:`tabsdatak.conn.kafka._serdes` subpackage; this module
hosts the ingester and the encoder that bridge ``confluent_kafka``
to the shared stream pipeline of
:mod:`tabsdatak.conn.common._stream`.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

import pyarrow as pa

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._encoder import StreamEncoder

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._ingester import StreamIngester

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offset import StreamOffset

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._offsets import SourceKeptOffsets

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.kafka import KafkaStreamSrc, KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes import (
    DecodeErrorKind,
    KafkaSerde,
    SchemaIdCompatibles,
    SchemaIdSpec,
    build_schema_registry_client,
    build_serde,
    classify_decode_error,
)
from tabsdatak.conn.kafka.constants import KAFKA_META_TOPIC
from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover -- typing-only
    from confluent_kafka import Consumer, Message, Producer, TopicPartition


logger = logging.getLogger(__name__)


# Confluent-kafka client cfg keys we set / inspect.
_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"
_AUTO_OFFSET_RESET_KEY = "auto.offset.reset"
_ENABLE_AUTO_COMMIT_KEY = "enable.auto.commit"
_SECURITY_PROTOCOL_KEY = "security.protocol"
_SASL_MECHANISM_KEY = "sasl.mechanism"
_SASL_USERNAME_KEY = "sasl.username"
_SASL_PASSWORD_KEY = "sasl.password"
_GROUP_ID_KEY = "group.id"
ASSIGNMENT_STRATEGY_KEY = "partition.assignment.strategy"

DEFAULT_CONNECT_TIMEOUT_SECS = 16
DEFAULT_ASSIGNMENT_STRATEGY = "cooperative-sticky"


def retriable_acknowledgement(error: BaseException) -> bool:
    from confluent_kafka import KafkaException

    return isinstance(error, KafkaException) and error.args[0].retriable()


@dataclass(frozen=True)
class KafkaRecord:
    value: dict | None
    schema_id: SchemaIdSpec | None
    size: int
    message: Message


class KafkaOffset(StreamOffset):
    def __init__(self, offsets: dict[str, dict[int, int]] | None = None) -> None:
        self.offsets: dict[str, dict[int, int]] = {
            topic: dict(partitions) for topic, partitions in (offsets or {}).items()
        }

    @classmethod
    def after(cls, message: Message) -> Self:
        return cls({message.topic(): {message.partition(): message.offset() + 1}})

    def update(self, other: Self) -> None:
        for topic, partitions in other.offsets.items():
            known = self.offsets.setdefault(topic, {})
            for partition, offset in partitions.items():
                known[partition] = max(offset, known.get(partition, offset))

    def write(self) -> str:
        return json.dumps({
            topic: {
                str(partition): offset
                for partition, offset in sorted(partitions.items())
            }
            for topic, partitions in sorted(self.offsets.items())
        })

    @classmethod
    def read(cls, data: str) -> Self:
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict):
                raise TypeError("The offset is not a map of topics")
            offsets: dict[str, dict[int, int]] = {}
            for topic, partitions in payload.items():
                if not isinstance(partitions, dict):
                    raise TypeError(f"The partitions of topic {topic!r} are not a map")
                offsets[topic] = {}
                for partition, offset in partitions.items():
                    number = int(partition)
                    if number < 0:
                        raise ValueError(f"The partition {partition!r} is negative")
                    if isinstance(offset, bool) or not isinstance(offset, int):
                        raise TypeError(f"The offset {offset!r} is not an integer")
                    if offset < 0:
                        raise ValueError(f"The offset {offset!r} is negative")
                    offsets[topic][number] = offset
        except (TypeError, ValueError) as error:
            raise KafkaErrorCode.KAFKA_25.exception(cause=error, state=data)
        return cls(offsets)

    def topic_partitions(self) -> list[TopicPartition]:
        from confluent_kafka import TopicPartition

        return [
            TopicPartition(topic, partition, offset)
            for topic, partitions in sorted(self.offsets.items())
            for partition, offset in sorted(partitions.items())
        ]


class KafkaEncoder(StreamEncoder[KafkaRecord, KafkaOffset]):
    def __init__(self, topic: str, compatible: SchemaIdCompatibles):
        self._topic = topic
        self._compatible = compatible

    def slot(self, record: KafkaRecord) -> str | None:
        return None if record.value is None else self._topic

    def size(self, record: KafkaRecord) -> int:
        return record.size

    def offset(self, record: KafkaRecord) -> KafkaOffset:
        return KafkaOffset.after(record.message)

    def encode(self, slot: str, records: list[KafkaRecord]) -> Iterator[pa.Table]:
        by_topic: dict[str, list[KafkaRecord]] = {}
        for record in records:
            by_topic.setdefault(record.message.topic(), []).append(record)
        for topic, of_topic in by_topic.items():
            group = [of_topic[0]]
            for record in of_topic[1:]:
                if self._compatible(group[0].schema_id, record.schema_id):
                    group.append(record)
                else:
                    yield self.encode_group(topic, group)
                    group = [record]
            yield self.encode_group(topic, group)

    def encode_group(self, topic: str, group: list[KafkaRecord]) -> pa.Table:
        values = [record.value for record in group]
        column_names = sorted({key for value in values for key in value})
        return pa.Table.from_pydict(
            mapping={
                KAFKA_META_TOPIC: [topic for _ in values],
                **{k: [value.get(k) for value in values] for k in column_names},
            },
        )


class KafkaIngester(StreamIngester[KafkaRecord, KafkaOffset]):
    def __init__(
        self,
        *,
        conn: KafkaStreamSrcConn,
        stream_src: KafkaStreamSrc,
        poll_max_secs: int,
        connect_timeout_secs: int,
    ):
        super().__init__()
        self.offsets = SourceKeptOffsets(
            acknowledger=self.acknowledge_offsets,
            retriable=retriable_acknowledgement,
        )
        self._conn = conn
        self._stream_src = stream_src
        self._poll_max_secs = poll_max_secs
        self._connect_timeout_secs = connect_timeout_secs
        self._consumer: Consumer | None = None
        self._dlq_producer: Producer | None = None
        self._dlq_failures: list[str] = []
        self._serde: KafkaSerde | None = None

    @property
    def topic(self) -> str:
        return self._stream_src.topic

    def schema_id_compatibles(self) -> SchemaIdCompatibles:
        return self._serde.schema_id_compatibles_checker()

    def open_session(self, work_dir: Path) -> Consumer:
        from confluent_kafka import Consumer

        kafka_configs = build_kafka_configs(conn=self._conn)
        try:
            consumer = Consumer(kafka_configs)
            consumer.list_topics(timeout=self._connect_timeout_secs)
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_6.exception(
                cause=exc,
                servers=kafka_configs.get(_BOOTSTRAP_SERVERS_KEY),
            )
        consumer.subscribe(
            [self.topic],
            on_revoke=self.partitions_revoked,
            on_lost=self.partitions_lost,
        )
        self._consumer = consumer

        self._dlq_producer = self.build_dlq_producer(kafka_configs)

        sr_client = build_schema_registry_client(
            schema_registry=self._conn.schema_registry,
        )
        sr_client_alt = None
        if sr_client is not None and self._stream_src.data_format not in (
            "json",
            "avro",
        ):
            sr_client_alt = build_schema_registry_client(
                schema_registry=self._conn.schema_registry,
            )
        self._serde = build_serde(
            data_format=self._stream_src.data_format,
            schema=self._stream_src.schema,
            schema_registry=self._conn.schema_registry,
            working_dir=str(work_dir),
            sr_client=sr_client,
            sr_client_alt=sr_client_alt,
        )
        return consumer

    def build_dlq_producer(self, kafka_configs: dict) -> Producer | None:
        if self._stream_src.dlq_topic is None:
            return None
        from confluent_kafka import Producer

        try:
            producer = Producer(kafka_configs)
            producer.list_topics(timeout=self._connect_timeout_secs)
            return producer
        except Exception as exc:
            raise KafkaErrorCode.KAFKA_7.exception(
                cause=exc,
                servers=kafka_configs.get(_BOOTSTRAP_SERVERS_KEY),
            )

    def partitions_revoked(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        if self._consumer is None:
            return
        logger.info(
            "The partitions %s are being revoked; staging and acknowledging "
            "everything pending first",
            [(tp.topic, tp.partition) for tp in partitions],
        )
        self.lifecycle.settle()

    def partitions_lost(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        raise KafkaErrorCode.KAFKA_30.exception(
            partitions=[(tp.topic, tp.partition) for tp in partitions],
        )

    def close_session(self, session: Consumer) -> None:
        self._consumer = None
        try:
            session.close()
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to close kafka consumer: %s", exc)
        if self._dlq_producer is not None:
            try:
                remaining = self._dlq_producer.flush(self._connect_timeout_secs)
            except Exception as exc:  # pragma: no cover
                logger.warning("Failed to flush DLQ producer: %s", exc)
            else:
                if remaining:
                    logger.warning(
                        "%s messages to the dead-letter topic were still "
                        "undelivered when the session closed",
                        remaining,
                    )

    def base_offset(self) -> KafkaOffset:
        return KafkaOffset()

    def acknowledge_offsets(self, offset: KafkaOffset) -> None:
        from confluent_kafka import KafkaException

        self.settle_dead_letters()
        owned = {(tp.topic, tp.partition) for tp in self._consumer.assignment()}
        partitions = [
            tp for tp in offset.topic_partitions() if (tp.topic, tp.partition) in owned
        ]
        if not partitions:
            return
        try:
            self._consumer.commit(offsets=partitions, asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].retriable():
                raise
            raise KafkaErrorCode.KAFKA_31.exception(cause=exc, error=exc.args[0])

    def settle_dead_letters(self) -> None:
        if self._dlq_producer is None:
            return
        undelivered = self._dlq_producer.flush(self._connect_timeout_secs)
        if undelivered or self._dlq_failures:
            raise self.dead_letter_failure(undelivered)

    def dead_letter_failure(
        self,
        undelivered: int,
        cause: BaseException | None = None,
    ) -> Exception:
        error = cause or (
            self._dlq_failures[0]
            if self._dlq_failures
            else "the broker did not acknowledge the delivery in time"
        )
        return KafkaErrorCode.KAFKA_29.exception(
            cause=cause,
            topic=self._stream_src.dlq_topic,
            undelivered=undelivered,
            failed=len(self._dlq_failures),
            error=error,
        )

    def poll_changes(self, session: Consumer) -> Iterator[KafkaRecord]:
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        deserialize = self._serde.deserializer_with_schema_id()
        start = time.time()
        while True:
            remaining = self._poll_max_secs - (time.time() - start)
            if remaining <= 0:
                return
            msg = session.poll(remaining)
            if msg is None:
                return
            err = msg.error()
            if err is not None:
                if err.fatal():
                    raise KafkaErrorCode.KAFKA_26.exception(error=err)
                logger.warning("The Kafka consumer reported an error: %s", err)
                continue
            try:
                record, schema_id = deserialize(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:  # noqa: BLE001 -- classified below
                if classify_decode_error(exc) is DecodeErrorKind.RETRYABLE:
                    raise KafkaErrorCode.KAFKA_27.exception(
                        cause=exc,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        error=exc,
                    )
                if not self._dlq_producer:
                    raise KafkaErrorCode.KAFKA_28.exception(
                        cause=exc,
                        topic=msg.topic(),
                        partition=msg.partition(),
                        offset=msg.offset(),
                        error=exc,
                    )
                logger.error(
                    "Failed to decode the message at offset %s of partition %s of "
                    "topic %r; sending it to the dead-letter topic: %s",
                    msg.offset(),
                    msg.partition(),
                    msg.topic(),
                    exc,
                )
                self.send_to_dlq(msg, exc)
                yield KafkaRecord(value=None, schema_id=None, size=0, message=msg)
                continue
            yield KafkaRecord(
                value=record,
                schema_id=schema_id,
                size=len(msg.value() or b""),
                message=msg,
            )

    def send_to_dlq(
        self,
        msg: Message,
        error: Exception | Any,
    ) -> None:
        if not (self._dlq_producer and self._stream_src.dlq_topic):
            return
        envelope = {
            "original_value": (
                msg.value().decode("utf-8", errors="replace")
                if msg.value() is not None
                else None
            ),
            "error": str(error),
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp(),
        }
        where = (
            f"offset {msg.offset()} of partition {msg.partition()} "
            f"of topic {msg.topic()!r}"
        )

        def on_delivery(delivery_error, _delivered):
            if delivery_error is not None:
                self._dlq_failures.append(f"{where}: {delivery_error}")

        def produce():
            self._dlq_producer.produce(
                topic=self._stream_src.dlq_topic,
                key=msg.key(),
                value=json.dumps(envelope).encode("utf-8"),
                on_delivery=on_delivery,
            )

        try:
            produce()
        except BufferError:
            self._dlq_producer.flush(self._connect_timeout_secs)
            try:
                produce()
            except BufferError as exc:
                raise self.dead_letter_failure(len(self._dlq_producer), cause=exc)
        self._dlq_producer.poll(0)
        if self._dlq_failures:
            raise self.dead_letter_failure(len(self._dlq_producer))


def build_kafka_configs(*, conn: KafkaStreamSrcConn) -> dict:
    """Compose the ``confluent_kafka.Consumer`` config dict from
    the connector's :class:`KafkaStreamSrcConn`.

    Resolves ``Secret`` references on ``servers`` and
    ``group_id``, resolves ``Secret`` SASL credentials, and forces
    ``auto.offset.reset=earliest`` + ``enable.auto.commit=False``
    (the plugin commits explicitly once each batch is staged)."""
    cfg: dict = dict(conn.conn_cfg or {})
    cfg[_BOOTSTRAP_SERVERS_KEY] = conn._servers()
    cfg[_AUTO_OFFSET_RESET_KEY] = "earliest"
    cfg[_ENABLE_AUTO_COMMIT_KEY] = False
    cfg.setdefault(ASSIGNMENT_STRATEGY_KEY, DEFAULT_ASSIGNMENT_STRATEGY)
    if conn.credentials is not None:
        cfg[_SECURITY_PROTOCOL_KEY] = "sasl_plaintext"
        cfg[_SASL_MECHANISM_KEY] = "SCRAM-SHA-256"
        cfg[_SASL_USERNAME_KEY] = _convert(conn.credentials.user, str)
        cfg[_SASL_PASSWORD_KEY] = _convert(conn.credentials.password, str)
    cfg[_GROUP_ID_KEY] = conn._group_id()
    return cfg

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
import sys
import time
from collections.abc import Callable, Iterator
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
from tabsdatak.conn.common._stream._restart import StreamRestart

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._size import size_of_values

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
METADATA_REFRESH_KEY = "topic.metadata.refresh.interval.ms"
FETCH_BACKOFF_KEY = "fetch.queue.backoff.ms"
KEEPALIVE_KEY = "socket.keepalive.enable"
POLL_INTERVAL_KEY = "max.poll.interval.ms"

DEFAULT_CONNECT_TIMEOUT_SECS = 16

# Fewer messages than this are decoded one by one: a batch decode has a fixed
# cost, and a trickle of messages does not need it.
MIN_BATCH_MESSAGES = 64
DEFAULT_ASSIGNMENT_STRATEGY = "cooperative-sticky"
DEFAULT_METADATA_REFRESH_MS = 30_000
DEFAULT_FETCH_BACKOFF_MS = 0
DEFAULT_POLL_INTERVAL_MS = 300_000
POLL_GAP_WARNING_RATIO = 0.5


def retriable_acknowledgement(error: BaseException) -> bool:
    from confluent_kafka import KafkaException

    return isinstance(error, KafkaException) and error.args[0].retriable()


@dataclass(frozen=True)
class KafkaRecord:
    """One message, decoded -- or, with `table` set, a whole fetched batch
    decoded at once: `rows` messages as one Arrow table, `after` the offset
    past the last of them in every partition, `partition` and `offset` those
    of the last message."""

    value: dict | None
    schema_id: SchemaIdSpec | None
    topic: str
    partition: int
    offset: int
    table: pa.Table | None = None
    rows: int = 1
    after: KafkaOffset | None = None

    def __sizeof__(self) -> int:
        if self.table is not None:
            return object.__sizeof__(self) + self.table.nbytes
        return object.__sizeof__(self) + size_of_values(self.value)


class KafkaOffset(StreamOffset):
    def __init__(self, offsets: dict[str, dict[int, int]] | None = None) -> None:
        self.offsets: dict[str, dict[int, int]] = {
            topic: dict(partitions) for topic, partitions in (offsets or {}).items()
        }

    @classmethod
    def after(cls, record: KafkaRecord) -> Self:
        return cls({record.topic: {record.partition: record.offset + 1}})

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
    def __init__(
        self,
        topic: str,
        compatible: SchemaIdCompatibles,
        reject: Callable[[KafkaRecord, Exception], None],
    ):
        self.topic = topic
        self.compatible = compatible
        self.reject = reject
        self.schemas: dict[tuple[str, SchemaIdSpec], pa.Schema] = {}

    def slot(self, record: KafkaRecord) -> str | None:
        if record.value is None and record.table is None:
            return None
        return self.topic

    def size(self, record: KafkaRecord) -> int:
        return sys.getsizeof(record)

    def offset(self, record: KafkaRecord) -> KafkaOffset:
        if record.after is not None:
            return record.after
        return KafkaOffset.after(record)

    def rows(self, record: KafkaRecord) -> int:
        return record.rows

    def encode(self, slot: str, records: list[KafkaRecord]) -> Iterator[pa.Table]:
        by_topic: dict[str, list[KafkaRecord]] = {}
        for record in records:
            by_topic.setdefault(record.topic, []).append(record)
        for topic, of_topic in by_topic.items():
            yield from self.encode_topic(topic, of_topic)

    def encode_topic(
        self, topic: str, records: list[KafkaRecord]
    ) -> Iterator[pa.Table]:
        """Decoded batches of one schema in a row join into one table; the
        messages around them group as compatible runs, one table each."""
        group: list[KafkaRecord] = []
        batches: list[KafkaRecord] = []
        for record in records:
            if record.table is not None:
                if group:
                    yield from self.encode_group(topic, group)
                    group = []
                if batches and batches[0].schema_id != record.schema_id:
                    yield self.encode_batches(topic, batches)
                    batches = []
                batches.append(record)
                continue
            if batches:
                yield self.encode_batches(topic, batches)
                batches = []
            if not group or self.compatible(group[0].schema_id, record.schema_id):
                group.append(record)
            else:
                yield from self.encode_group(topic, group)
                group = [record]
        if batches:
            yield self.encode_batches(topic, batches)
        if group:
            yield from self.encode_group(topic, group)

    def encode_group(self, topic: str, group: list[KafkaRecord]) -> Iterator[pa.Table]:
        key = (topic, group[0].schema_id)
        known = self.schemas.get(key)
        try:
            table = self.assemble([record.value for record in group], known)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            table = self.assemble_each(group, known)
            if table is None:
                return
        yield self.finish(key, table)

    def encode_batches(self, topic: str, batches: list[KafkaRecord]) -> pa.Table:
        """Batches of one schema id, laid out as a run of their messages would
        be: the columns sorted, the types widened to the slot's known schema."""
        tables = [record.table for record in batches]
        table = tables[0] if len(tables) == 1 else pa.concat_tables(tables)
        table = table.select(sorted(table.column_names))
        key = (topic, batches[0].schema_id)
        known = self.schemas.get(key)
        if known is not None:
            columns = [
                self.widen(table.column(name), known.field(name), name)
                if name in known.names
                else table.column(name)
                for name in table.column_names
            ]
            table = pa.table(dict(zip(table.column_names, columns)))
        return self.finish(key, table)

    def finish(self, key: tuple[str, SchemaIdSpec], table: pa.Table) -> pa.Table:
        self.schemas[key] = table.schema
        topic = pa.array([key[0]] * table.num_rows)
        return table.add_column(0, KAFKA_META_TOPIC, topic)

    @staticmethod
    def widen(column: pa.ChunkedArray, known: pa.Field, name: str) -> pa.ChunkedArray:
        unified = pa.unify_schemas(
            [pa.schema([known]), pa.schema([pa.field(name, column.type)])],
            promote_options="permissive",
        )
        return column.cast(unified.field(name).type)

    def assemble(self, rows: list[dict], known: pa.Schema | None) -> pa.Table:
        names = {name for row in rows for name in row}
        if known is not None:
            names |= set(known.names)
        columns = {}
        for name in sorted(names):
            array = pa.array([row.get(name) for row in rows])
            if known is not None and name in known.names:
                array = self.widen(array, known.field(name), name)
            columns[name] = array
        return pa.table(columns)

    def assemble_each(
        self,
        group: list[KafkaRecord],
        known: pa.Schema | None,
    ) -> pa.Table | None:
        tables = []
        schema = known
        for record in group:
            try:
                table = self.assemble([record.value], schema)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
                self.reject(record, error)
                continue
            schema = table.schema
            tables.append(table)
        if not tables:
            return None
        return pa.concat_tables(tables, promote_options="permissive")


class KafkaIngester(StreamIngester[KafkaRecord, KafkaOffset]):
    def __init__(
        self,
        *,
        conn: KafkaStreamSrcConn,
        stream_src: KafkaStreamSrc,
        poll_max_secs: int,
        connect_timeout_secs: int,
        consume_max_messages: int,
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
        self._consume_max_messages = consume_max_messages
        self._contexts: dict[str, Any] = {}
        self._consumer: Consumer | None = None
        self._dlq_producer: Producer | None = None
        self._dlq_failures: list[str] = []
        self._serde: KafkaSerde | None = None
        self._batch_decode = None
        self._lost: list[TopicPartition] | None = None
        self._polled_at: float | None = None
        self._poll_interval_secs: float = DEFAULT_POLL_INTERVAL_MS / 1000

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
        self._lost = None
        self._polled_at = None
        self._poll_interval_secs = (
            int(kafka_configs.get(POLL_INTERVAL_KEY, DEFAULT_POLL_INTERVAL_MS)) / 1000
        )

        self._dlq_failures = []
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
        self._batch_decode = self._serde.batch_decoder()
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
        self._lost = list(partitions)

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
            raise KafkaErrorCode.KAFKA_30.exception(cause=exc, error=exc.args[0])

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

    def check_lost(self) -> None:
        if self._lost is None:
            return
        partitions = [(tp.topic, tp.partition) for tp in self._lost]
        self._lost = None
        raise StreamRestart(
            f"the consumer lost the partitions {partitions} to the group"
        )

    def watch_poll_gap(self, now: float) -> None:
        if self._polled_at is None:
            return
        gap = now - self._polled_at
        if gap > POLL_GAP_WARNING_RATIO * self._poll_interval_secs:
            logger.warning(
                "The consumer went %.0f seconds without polling, more than half of "
                "the %.0f seconds of %s; a longer stage would evict it from the "
                "group, raise %s in conn_cfg if the stages take this long",
                gap,
                self._poll_interval_secs,
                POLL_INTERVAL_KEY,
                POLL_INTERVAL_KEY,
            )

    def poll_changes(self, session: Consumer) -> Iterator[KafkaRecord]:
        start = time.time()
        self.watch_poll_gap(start)
        try:
            yield from self.poll_messages(session, start)
        finally:
            self._polled_at = time.time()

    def poll_messages(self, session: Consumer, start: float) -> Iterator[KafkaRecord]:
        while True:
            self.check_lost()
            remaining = self._poll_max_secs - (time.time() - start)
            if remaining <= 0:
                return
            messages = session.consume(
                num_messages=self._consume_max_messages,
                timeout=remaining,
            )
            self.check_lost()
            if not messages:
                return
            yield from self.decode_batches(messages)

    def decode_batches(self, messages: list[Message]) -> Iterator[KafkaRecord]:
        """Each topic's share of the fetch as one record when the serde can
        decode it at once; otherwise its messages one by one."""
        by_topic: dict[str, list[Message]] = {}
        for msg in messages:
            by_topic.setdefault(msg.topic(), []).append(msg)
        for topic, of_topic in by_topic.items():
            batch = self.decode_batch(topic, of_topic)
            if batch is not None:
                yield batch
            else:
                yield from self.decode_messages(of_topic)

    def decode_batch(self, topic: str, messages: list[Message]) -> KafkaRecord | None:
        decode = self._batch_decode
        if decode is None or len(messages) < MIN_BATCH_MESSAGES:
            return None
        values = []
        offsets: dict[int, int] = {}
        for msg in messages:
            if msg.error() is not None:
                return None
            values.append(msg.value())
            offsets[msg.partition()] = msg.offset() + 1
        table = decode(values)
        if table is None:
            return None
        last = messages[-1]
        return KafkaRecord(
            value=None,
            schema_id=tuple(sorted(table.column_names)),
            topic=topic,
            partition=last.partition(),
            offset=last.offset(),
            table=table,
            rows=table.num_rows,
            after=KafkaOffset({topic: offsets}),
        )

    def context(self, topic: str):
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        context = self._contexts.get(topic)
        if context is None:
            context = SerializationContext(topic, MessageField.VALUE)
            self._contexts[topic] = context
        return context

    def decode_messages(self, messages: list[Message]) -> Iterator[KafkaRecord]:
        deserialize = self._serde.deserializer_with_schema_id()
        for msg in messages:
            err = msg.error()
            if err is not None:
                if err.fatal():
                    raise KafkaErrorCode.KAFKA_26.exception(error=err)
                logger.warning("The Kafka consumer reported an error: %s", err)
                continue
            try:
                record, schema_id = deserialize(msg.value(), self.context(msg.topic()))
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
                yield KafkaRecord(
                    value=None,
                    schema_id=None,
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                continue
            yield KafkaRecord(
                value=record,
                schema_id=schema_id,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
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
        self.produce_dead_letter(msg.key(), envelope, where)

    def reject_record(self, record: KafkaRecord, error: Exception) -> None:
        if not self._dlq_producer:
            raise KafkaErrorCode.KAFKA_31.exception(
                cause=error,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error=error,
            )
        logger.error(
            "The record at offset %s of partition %s of topic %r does not fit the "
            "table's schema; sending it to the dead-letter topic: %s",
            record.offset,
            record.partition,
            record.topic,
            error,
        )
        envelope = {
            "original_value": None,
            "record": record.value,
            "error": str(error),
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "timestamp": None,
        }
        where = (
            f"offset {record.offset} of partition {record.partition} "
            f"of topic {record.topic!r}"
        )
        self.produce_dead_letter(None, envelope, where)

    def produce_dead_letter(self, key, envelope: dict, where: str) -> None:
        def on_delivery(delivery_error, _delivered):
            if delivery_error is not None:
                self._dlq_failures.append(f"{where}: {delivery_error}")

        def produce():
            self._dlq_producer.produce(
                topic=self._stream_src.dlq_topic,
                key=key,
                value=json.dumps(envelope, default=str).encode("utf-8"),
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
    cfg.setdefault(METADATA_REFRESH_KEY, DEFAULT_METADATA_REFRESH_MS)
    cfg.setdefault(FETCH_BACKOFF_KEY, DEFAULT_FETCH_BACKOFF_MS)
    cfg.setdefault(KEEPALIVE_KEY, True)
    if conn.credentials is not None:
        cfg[_SECURITY_PROTOCOL_KEY] = "sasl_plaintext"
        cfg[_SASL_MECHANISM_KEY] = "SCRAM-SHA-256"
        cfg[_SASL_USERNAME_KEY] = _convert(conn.credentials.user, str)
        cfg[_SASL_PASSWORD_KEY] = _convert(conn.credentials.password, str)
    cfg[_GROUP_ID_KEY] = conn._group_id()
    return cfg

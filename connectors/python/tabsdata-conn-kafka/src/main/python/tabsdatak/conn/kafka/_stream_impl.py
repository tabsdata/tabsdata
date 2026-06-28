"""Kafka stream-source consumer + buffer.

The wire-format (de)serialisers live in the
:mod:`tabsdatak.conn.kafka._serdes` subpackage; this module
hosts the polling consumer wrapper and the schema-compatible
message buffer that bridge ``confluent_kafka`` to the runtime's
manifest-stage contract.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.kafka import KafkaStreamSrcConn

# noinspection PyProtectedMember
from tabsdatak.conn.kafka._serdes import KafkaSerde, SchemaIdCompatibles, SchemaIdSpec

if TYPE_CHECKING:  # pragma: no cover -- typing-only
    from confluent_kafka import Consumer, Message, Producer


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

DEFAULT_TIME_ROLLOVER_SECS = 60
DEFAULT_CONSUME_TIMEOUT_SECS = 2
DEFAULT_CONNECTION_TIMEOUT_SECS = 16


class RolloverReason(Enum):
    """Why the consumer just closed the current buffer:

    * ``SchemaChange`` -- a message with an incompatible schema
      arrived and was bumped to a new buffer.
    * ``Size`` -- buffer hit its byte threshold.
    * ``Messages`` -- buffer hit its message-count threshold.
    * ``Time`` -- the poll-deadline elapsed; flush what we have.
    """

    SchemaChange = "SchemaChange"
    Size = "Size"
    Messages = "Messages"
    Time = "Time"


# ---------------------------------------------------------------------
# Messages buffer
# ---------------------------------------------------------------------


class MessagesBuffer:
    """Schema-compatible buffer of deserialised messages.

    Maintains the current ``schema_id`` (set on first append).
    Subsequent appends with the same / compatible id grow the
    buffer; incompatible ids fail the append (caller rolls over).
    :meth:`to_parquet` writes the buffer to a fresh parquet file
    under ``dir_path`` and marks the buffer flushed."""

    def __init__(
        self,
        topic: str,
        schema_ids_compatibles_checker: SchemaIdCompatibles,
    ):
        self._topic = topic
        self._schema_ids_compatibles_checker = schema_ids_compatibles_checker
        self._messages: list[dict] = []
        self._schema_id: SchemaIdSpec | None = None
        self._size_bytes = 0
        self._flushed = False

    def append(self, entry: tuple[int, dict, SchemaIdSpec]) -> bool:
        data_size, message, schema_id = entry
        if self._schema_id is None:
            self._messages.append(message)
            self._schema_id = schema_id
            self._size_bytes += data_size
            return True
        if self._schema_ids_compatibles_checker(self._schema_id, schema_id):
            self._messages.append(message)
            self._size_bytes += data_size
            return True
        return False

    @property
    def message_count(self) -> int:
        return len(self._messages)

    @property
    def size_mb(self) -> int:
        return int(self._size_bytes / (1024 * 1024))

    @property
    def is_flushed(self) -> bool:
        return self._flushed

    def to_parquet(self, dir_path: str | Path) -> Path | None:
        """Write the buffered messages to a parquet file. Returns
        ``None`` if the buffer is empty -- caller treats this as
        a no-op rollover."""
        if not self._messages:
            return None
        file_path = (
            Path(dir_path)
            / f"{self._topic}_{int(time.time())}_{uuid.uuid4().hex[:8]}.parquet"
        )
        import pyarrow as pa
        import pyarrow.parquet as pq

        column_names = sorted(self._messages[0].keys())
        table = pa.Table.from_pydict(
            mapping={k: [m.get(k) for m in self._messages] for k in column_names},
        )
        pq.write_table(table, file_path)
        self._flushed = True
        return file_path


# ---------------------------------------------------------------------
# Consumer wrapper
# ---------------------------------------------------------------------


class KafkaConsumer:
    """Thin wrapper around ``confluent_kafka.Consumer`` that drives
    a :class:`MessagesBuffer` + rollover decision per poll cycle.

    Each :meth:`consume` call returns when one of the rollover
    conditions fires (time / size / messages / schema-change).
    """

    def __init__(
        self,
        *,
        topic: str,
        consumer: "Consumer",
        serde: KafkaSerde,
        size_rollover_mb: int | None = None,
        messages_rollover: int | None = None,
        dlq_producer: "Producer | None" = None,
        dlq_topic: str | None = None,
    ):
        self._topic = topic
        self._consumer = consumer
        self._serde = serde
        self._size_rollover_mb = size_rollover_mb
        self._messages_rollover = messages_rollover
        self._dlq_producer = dlq_producer
        self._dlq_topic = dlq_topic
        self._buffer = self._new_buffer()
        self._commit_message: "Message | None" = None

    def _new_buffer(self) -> MessagesBuffer:
        return MessagesBuffer(
            self._topic,
            self._serde.schema_id_compatibles_checker(),
        )

    @property
    def buffer(self) -> MessagesBuffer:
        return self._buffer

    def consume(
        self,
        *,
        timeout_secs: int,
        reset_buffer: bool = False,
    ) -> tuple[RolloverReason, MessagesBuffer]:
        """Poll up to ``timeout_secs``; return on the first
        rollover trigger. ``reset_buffer`` discards the current
        buffer before polling (used after a time rollover that the
        runner already staged)."""
        from confluent_kafka.serialization import (
            MessageField,
            SerializationContext,
        )

        if reset_buffer:
            self._buffer = self._new_buffer()

        start = time.time()
        while True:
            remaining = timeout_secs - (time.time() - start)
            if remaining <= 0:
                return RolloverReason.Time, self._buffer
            msg = self._consumer.poll(remaining)
            if msg is None:
                return RolloverReason.Time, self._buffer
            err = msg.error()
            if err is not None:
                logger.error("Kafka error: %s", err)
                self._send_to_dlq(msg, err)
                continue
            try:
                record, schema_id = self._serde.deserializer_with_schema_id()(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as exc:  # noqa: BLE001 -- surface to DLQ
                logger.error(
                    "Failed to deserialise message at offset %s: %s",
                    msg.offset(),
                    exc,
                )
                self._send_to_dlq(msg, exc)
                continue
            entry = (len(msg.value() or b""), record, schema_id)
            if not self._buffer.append(entry):
                # Schema change: hand the old buffer back, start a
                # fresh one that owns this message.
                rolled = self._buffer
                self._buffer = self._new_buffer()
                self._buffer.append(entry)
                self._commit_message = msg
                return RolloverReason.SchemaChange, rolled
            self._commit_message = msg
            if (
                self._messages_rollover is not None
                and self._buffer.message_count >= self._messages_rollover
            ):
                rolled = self._buffer
                self._buffer = self._new_buffer()
                return RolloverReason.Messages, rolled
            if (
                self._size_rollover_mb is not None
                and self._buffer.size_mb >= self._size_rollover_mb
            ):
                rolled = self._buffer
                self._buffer = self._new_buffer()
                return RolloverReason.Size, rolled

    def _send_to_dlq(
        self,
        msg: "Message",
        error: Exception | Any,
    ) -> None:
        if not (self._dlq_producer and self._dlq_topic):
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
        self._dlq_producer.produce(
            topic=self._dlq_topic,
            key=msg.key(),
            value=json.dumps(envelope).encode("utf-8"),
        )
        self._dlq_producer.poll(0)
        self._dlq_producer.flush()

    def commit(self) -> None:
        """Sync-commit the last successfully-buffered offset."""
        if self._commit_message is not None:
            self._consumer.commit(
                message=self._commit_message,
                asynchronous=False,
            )
            self._commit_message = None

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to close kafka consumer: %s", exc)


# ---------------------------------------------------------------------
# Kafka configs builder
# ---------------------------------------------------------------------


def build_kafka_configs(*, conn: KafkaStreamSrcConn) -> dict:
    """Compose the ``confluent_kafka.Consumer`` config dict from
    the connector's :class:`KafkaStreamSrcConn`.

    Resolves ``Var`` / ``Secret`` references on ``servers`` and
    ``group_id``, resolves ``Secret`` SASL credentials, and forces
    ``auto.offset.reset=earliest`` + ``enable.auto.commit=False``
    (the plugin commits explicitly after each rollover)."""
    cfg: dict = dict(conn.conn_cfg or {})
    cfg[_BOOTSTRAP_SERVERS_KEY] = _convert(conn.servers, str)
    cfg[_AUTO_OFFSET_RESET_KEY] = "earliest"
    cfg[_ENABLE_AUTO_COMMIT_KEY] = False
    if conn.credentials is not None:
        cfg[_SECURITY_PROTOCOL_KEY] = "sasl_plaintext"
        cfg[_SASL_MECHANISM_KEY] = "SCRAM-SHA-256"
        cfg[_SASL_USERNAME_KEY] = _convert(conn.credentials.user, str)
        cfg[_SASL_PASSWORD_KEY] = _convert(conn.credentials.password, str)
    cfg[_GROUP_ID_KEY] = _convert(conn.group_id, str)
    return cfg

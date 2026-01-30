#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
import time
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from confluent_kafka import Consumer, KafkaError, Message, Producer


from tabsdata_kafka.constants import CONNECTION_TIMEOUT_SECS
from tabsdata_kafka.consumer.messages_buffer import MessagesBuffer
from tabsdata_kafka.serdes.serde import KafkaSerde

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class KafkaConsumer:
    """
    A Kafka consumer that groups messages by schema compatibility and delivers them in
    batches.

    This consumer polls messages from a Kafka topic, deserializes them, and buffers
    them.
    The buffer is "rolled over" (a new one is created and the old one is returned) when
    one of the configured rollover conditions is met (e.g., buffer size, number of
    messages, or a timeout is reached). It also rolls over when a message with an
    incompatible schema is received.
    """

    def __init__(
        self,
        topic: str,
        consumer: Consumer,
        serde: KafkaSerde,
        *,
        size_rollover_mb: int = None,
        messages_rollover: int = None,
        dlq_producer: Producer | None = None,
        dlq_topic: str | None = None,
    ):
        """
        :param topic: the Kafka topic to consume from
        :param consumer: a pre-configured `confluent_kafka.Consumer` instance.
                         Must be already subscribed to the topic.
        :param serde: a `KafkaSerde` instance for deserialization
        :param size_rollover_mb: an optional `SizeRollover` condition
        :param messages_rollover: an optional `MessagesRollover` condition
        """
        self._topic = topic
        # Consumer must be already subscribed to topic and must be the same topic given
        # here
        self._consumer = consumer
        self._serde = serde
        self._size_rollover_mb = size_rollover_mb
        self._messages_rollover = messages_rollover
        self._buffer = self._new_buffer()
        self._commit_message = None
        self._dlq_producer = dlq_producer
        self._dlq_topic = dlq_topic

    def _new_buffer(self) -> MessagesBuffer:
        return MessagesBuffer(self._topic, self._serde.schema_id_compatibles_checker())

    def _send_to_dlq(self, msg: Message, error: Exception | KafkaError):
        if self._dlq_producer and self._dlq_topic:
            dlq_message = {
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
                value=json.dumps(dlq_message).encode("utf-8"),
                callback=lambda err, _msg: logger.debug(
                    f"DLQ delivery failed: {err}" if err else "DLQ message delivered"
                ),
            )
            # Force timeout if DLQ is not reachable
            self._dlq_producer.list_topics(timeout=CONNECTION_TIMEOUT_SECS)
            self._dlq_producer.poll(0)
            self._dlq_producer.flush()

    def consume(
        self, timeout_secs: int, reset_buffer: bool = False
    ) -> tuple[RolloverReason, MessagesBuffer]:
        """
        Consumes messages from the Kafka topic and buffers them.

        This method will block for at most `timeout_secs`. It returns a buffer of
        messages when one of the rollover conditions is met.

        Args:
            timeout_secs: Maximum time to block while consuming messages.
            reset_buffer: If True, resets the current buffer before consuming.
        Returns:
            A tuple containing the reason for the rollover and the buffered messages.
        """
        if reset_buffer:
            self._buffer = self._new_buffer()

        start = time.time()
        while True:
            max_timeout = timeout_secs - (time.time() - start)
            msg = self._consumer.poll(max_timeout)
            if self._is_timeout(start, timeout_secs, msg):
                return RolloverReason.Time, self._buffer
            if msg.error():
                from confluent_kafka import KafkaError

                # noinspection PyProtectedMember,PyUnresolvedReferences
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # continue polling
                else:
                    logger.error(f"Kafka Error: {msg.error()}")
                    self._send_to_dlq(msg, msg.error())
                    continue  # continue polling
            try:
                rollover_reason = self._process_message(msg)
                if rollover_reason:
                    return rollover_reason
            except Exception as e:
                logger.error(
                    f"Error occurred while consuming message {msg.value()}: {e}"
                )
                self._send_to_dlq(msg, e)
                continue
        logger.error("Exited consume loop unexpectedly")
        raise RuntimeError("Exited consume loop unexpectedly")

    @staticmethod
    def _is_timeout(start, timeout_secs, msg):
        if time.time() - start > timeout_secs or msg is None:
            return True
        return False

    def _handle_error(self, msg):
        from confluent_kafka import KafkaError

        # noinspection PyProtectedMember,PyUnresolvedReferences
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return True  # continue polling
        else:
            logger.error(f"Kafka Error: {msg.error()}")
            self._send_to_dlq(msg, msg.error())
            return True  # continue polling

    def _process_message(self, msg):
        from confluent_kafka.serialization import MessageField, SerializationContext

        data = msg.value()
        (record, schema_id) = self._serde.deserializer_with_schema_id()(
            data, SerializationContext(msg.topic(), MessageField.VALUE)
        )
        entry = (len(data), record, schema_id)
        if not self._buffer.append(entry):
            message_buffer = self._buffer
            self._buffer = self._new_buffer()
            self._buffer.append(entry)
            return RolloverReason.SchemaChange, message_buffer

        self._commit_message = msg

        if (
            self._messages_rollover
            and self._buffer.message_count >= self._messages_rollover
        ):
            message_buffer = self._buffer
            self._buffer = self._new_buffer()
            return RolloverReason.Messages, message_buffer

        if self._size_rollover_mb and self._buffer.size_mb >= self._size_rollover_mb:
            message_buffer = self._buffer
            self._buffer = self._new_buffer()
            return RolloverReason.Size, message_buffer

        return None

    def commit(self):
        """
        Commits the offset of the last message that was successfully added to a buffer.
        This is a synchronous commit.
        """
        if self._commit_message:
            self._consumer.commit(message=self._commit_message, asynchronous=False)
            self._commit_message = None

    def close(self):
        """
        Closes the underlying Kafka consumer and DLQ topic producer, if provided.
        """
        self._consumer.close()

    def __repr__(self):
        return (
            f"KafkaConsumer(topic={self._topic}, consumer={self._consumer},"
            f" serde={self._serde}, size_rollover_mb={self._size_rollover_mb},"
            f" messages_rollover={self._messages_rollover}),"
            f" dlq_topic={self._dlq_topic}), dlq_producer={self._dlq_producer},"
            f" buffer={self._buffer})"
        )


class RolloverReason(Enum):
    """
    Enumerates the reasons for a buffer rollover.
    """

    SchemaChange = 1, "A message with an incompatible schema was received"
    Size = 2, "The buffer size limit was reached"
    Messages = 3, "The message count limit was reached"
    Time = 4, "The consume timeout was reached"

    @property
    def code(self) -> int:
        return self.value[0]

    @property
    def description(self) -> str:
        return self.value[1]

#
# Copyright 2026 Tabs Data Inc.
#
from __future__ import annotations

import json
import logging
import time
from enum import Enum
from typing import Tuple

from confluent_kafka import Consumer, KafkaError, Message, Producer
from confluent_kafka.serialization import MessageField, SerializationContext

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

    def _send_to_dlq(self, msg: Message, error: Exception):
        if self._dlq_producer and self._dlq_topic:
            dlq_message = {
                "original_value": msg.value(),
                "error": str(error),
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp(),
            }
            self._dlq_producer.produce(
                topic=self._dlq_topic,
                key=str(msg.key()),
                value=json.dumps(dlq_message).encode("utf-8"),
                callback=lambda err, _msg: logger.debug(
                    f"DLQ delivery failed: {err}" if err else "DLQ message delivered"
                ),
            )
            self._dlq_producer.poll(0)
            self._dlq_producer.flush()

    # ToDo: Simplify or reorganize to make liter exception unnecessary.
    def consume(  # noqa: C901
        self, timeout_secs: int, reset_buffer: bool = False
    ) -> Tuple[RolloverReason, MessagesBuffer]:
        """
        Consumes messages from the Kafka topic and buffers them.

        This method will block for at most `timeout_secs`. It returns a buffer of
        messages when one of the rollover conditions is met.

        :param timeout_secs: the maximum time to wait for messages before returning
        :param reset_buffer: if the buffer should be reset to empty or kept.
        :return: a tuple containing the `RolloverReason` and the `MessagesBuffer`
        :raises: "Cannot consume before a commit is done"
        """
        if reset_buffer:
            self._buffer = self._new_buffer()

        start = time.time()  # we keep track of the start time to be able to timeout
        while True:
            max_timeout = timeout_secs - (
                time.time() - start
            )  # adjusting kafka timeout based on the remaining timeout
            msg = self._consumer.poll(max_timeout)
            # logger.debug(f"Polled message: {msg}")
            if time.time() - start > timeout_secs:  # timeout
                return RolloverReason.Time, self._buffer
            if msg is None:
                return RolloverReason.Time, self._buffer
            if msg.error():
                # noinspection PyProtectedMember,PyUnresolvedReferences
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka Error: {msg.error()}")
                    self._send_to_dlq(msg, msg.error())
                    continue

            try:
                # noinspection PyArgumentList
                data = msg.value()

                (record, schema_id) = self._serde.deserializer_with_schema_id()(
                    data, SerializationContext(msg.topic(), MessageField.VALUE)
                )

                entry = (len(data), record, schema_id)
                if not self._buffer.append(entry):
                    message_buffer = self._buffer
                    self._buffer = self._new_buffer()
                    self._buffer.append(entry)
                    # We don't update the commit message a the message is in the new
                    # buffer
                    return RolloverReason.SchemaChange, message_buffer

                # We update the commit message a message has been added to the current
                # buffer
                self._commit_message = msg

                if self._messages_rollover:
                    if self._buffer.message_count() >= self._messages_rollover:
                        message_buffer = self._buffer
                        self._buffer = self._new_buffer()
                        return RolloverReason.Messages, message_buffer

                if self._size_rollover_mb:
                    if self._buffer.size_mb() >= self._size_rollover_mb:
                        message_buffer = self._buffer
                        self._buffer = self._new_buffer()
                        return RolloverReason.Size, message_buffer
            except Exception as e:
                logger.error(
                    f"Error occurred while consuming message {msg.value()}: {e}"
                )
                self._send_to_dlq(msg, e)
                continue
        # noinspection PyTypeChecker
        # We never reach here
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


class RolloverReason(Enum):
    """
    Enumerates the reasons for a buffer rollover.
    """

    SchemaChange = 1, "A message with an incompatible schema was received"
    Size = 2, "The buffer size limit was reached"
    Messages = 3, "The message count limit was reached"
    Time = 4, "The consume timeout was reached"

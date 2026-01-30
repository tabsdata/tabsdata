#
# Copyright 2026 Tabs Data Inc.
#

import time
import uuid
from pathlib import Path

from tabsdata_kafka.typing import SchemaIdCompatibles, SchemaIdSpec


class MessagesBuffer:
    """
    Holds a buffer of Kafka messages that are compatible with a specific schema.
    This is used to aggregate messages before processing or storing them in bulk,
    for example in a parquet file.
    """

    def __init__(self, topic: str, schema_ids_compatibles_checker: SchemaIdCompatibles):
        self._topic = topic
        self._schema_ids_compatibles_checker = schema_ids_compatibles_checker
        self._messages = []
        self._schema_id = None
        self._size = 0
        self._flushed = False

    def append(self, message_schema_id: tuple[int, dict, SchemaIdSpec]) -> bool:
        """
        Appends a message to the buffer if its schema is compatible with the existing
        messages.
        When using Schema Registry Serdes, a schema is compatible if they are the same.
        When not using Schema Registry Serdes, for JSON and AVRO if the schema of
        messages in the buffer contains
        the message being appended, the schemas are compatible otherwise they are not.
        For PROTOBUF, only if the
        schemas are the same
        :param message_schema_id: a tuple containing the message size, the deserialized
        message and the schema id
        :return: True if the message was appended, False otherwise
        """
        data_size, message, schema_id = message_schema_id
        if not self._schema_id:
            self._messages.append(message)
            self._schema_id = schema_id
            self._size += data_size
            return True
        elif self._schema_ids_compatibles_checker(self._schema_id, schema_id):
            self._messages.append(message)
            self._size += data_size
            return True
        else:
            return False

    @property
    def messages(self) -> list[dict]:
        """
        Returns the list of messages in the buffer.
        """
        return self._messages

    @property
    def schema_id(self) -> SchemaIdSpec:
        """
        Returns the schema ID of the messages in the buffer.
        """
        return self._schema_id

    @property
    def message_count(self) -> int:
        """
        Returns the number of messages in the buffer.
        """
        return len(self._messages)

    @property
    def size_mb(self) -> int:
        """
        Returns the approximate size of the messages in the buffer in megabytes.
        """
        return int(self._size / (1024 * 1024))

    @property
    def is_flushed(self):
        """
        Returns whether the buffer has been flushed to a Parquet file."""
        return self._flushed

    def to_parquet(self, dir_path: str) -> Path | None:
        """
        Writes the buffered messages to a Parquet file.
        :param dir_path: the directory where the file will be saved
        :return: the path to the created Parquet file, or None if there are no messages
            to write
        """

        if self.message_count == 0:
            return None

        file_path = (
            Path(dir_path)
            / f"{self._topic}_{int(time.time())}_{str(uuid.uuid4())[:8]}.parquet"
        )

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Pyarrow infers schema from the list of dicts
        # all messages have the same column names or a subset of the colum names of the
        # first one
        # we sort the column names so the order of columns in the parquet files are
        # always the same
        # for the same column names, thus 'same schema'
        column_names = sorted(list(self.messages[0].keys()))

        table = pa.Table.from_pydict(
            {k: [message.get(k) for message in self.messages] for k in column_names}
        )
        pq.write_table(table, file_path)
        self._flushed = True
        return file_path

    def __repr__(self):
        return (
            f"MessagesBuffer(topic={self._topic}, schema_id={self._schema_id},"
            f" message_count={self.message_count}, size_mb={self.size_mb})"
        )

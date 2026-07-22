#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias, Union

if TYPE_CHECKING:
    from confluent_kafka.serialization import SerializationContext

MsgFormatSpec: TypeAlias = Literal["avro", "json", "protobuf"]

SchemaIdSpec: TypeAlias = Union[str, dict[str, str]]

SchemaIdCompatibles: TypeAlias = Callable[[SchemaIdSpec, SchemaIdSpec], bool]
"""
The schemas are the same or the first one contains the second one.
"""


class KafkaSerde(ABC):
    """
    An abstract class for creating Kafka serializers and deserializers.
    """

    @abstractmethod
    def __init__(self):
        def deserializer_with_schema_id(
            data: bytes, ctx: SerializationContext
        ) -> tuple[dict, SchemaIdSpec]:
            message = self.deserializer()(data, ctx)
            schema_id = self.extract_schema_id(data, message)
            return message, schema_id

        self._deserializer_with_schema_id = deserializer_with_schema_id

    @property
    @abstractmethod
    def format(self) -> MsgFormatSpec:
        """
        Returns the message format.
        """
        pass

    @abstractmethod
    def serializer(self) -> Callable[[dict, SerializationContext], bytes]:
        """
        Returns a serializer function that takes an object and a SerializationContext
        and returns bytes.
        """
        pass

    @abstractmethod
    def deserializer(self) -> Callable[[bytes, SerializationContext], dict]:
        """
        Returns a deserializer function that takes bytes and a SerializationContext
        and returns a dict for the message.
        """
        pass

    @abstractmethod
    def extract_schema_id(self, data: bytes, message: dict) -> SchemaIdSpec:
        """
        Returns the Schema ID of the message.
        :param data: raw message.
        :param message: deserialized message.
        :return: The Schema ID of the message.
        """
        pass

    def deserializer_with_schema_id(
        self,
    ) -> Callable[[bytes, SerializationContext], tuple[dict, SchemaIdSpec]]:
        """
        Returns a deserializer function that takes bytes and a SerializationContext
        and returns a dict and the schema_id for the message.
        """
        return self._deserializer_with_schema_id

    @abstractmethod
    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles:
        """
        Returns a Schema ID compatibility checker for Schema IDs returned by this serde.
        NOTE: Compatibility means, the schemas are the same or the first one contains
        the second one.
        :return: the schema ID compatibility checker.
        """
        pass

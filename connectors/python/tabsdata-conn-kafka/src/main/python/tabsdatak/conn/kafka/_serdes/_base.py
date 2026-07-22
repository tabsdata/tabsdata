#
# Copyright 2026 Tabsdata Inc.
#

"""Abstract base for Kafka (de)serialisers.

Every concrete serde implements:

* :meth:`format` -- ``"avro"`` / ``"json"`` / ``"protobuf"``.
* :meth:`serializer` -- callable that bytes-encodes a dict.
* :meth:`deserializer` -- callable that dict-decodes bytes.
* :meth:`extract_schema_id` -- the buffer-compatibility key the
  ``MessagesBuffer`` groups on. SR-aware serdes pull this from
  the wire header; no-SR serdes derive it from the message's
  field set.
* :meth:`schema_id_compatibles_checker` -- equality / subset
  predicate over the schema-ids the buffer uses to decide
  whether a new message can join the current batch.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias, Union

if TYPE_CHECKING:  # pragma: no cover -- typing-only
    from confluent_kafka.serialization import SerializationContext


# Wire-format identifier the serdes report.
MsgFormatSpec: TypeAlias = Literal["avro", "json", "protobuf"]

# The schema id a serde reports per message. SR-aware serdes
# report a stringified registry id (Confluent: ``str(int)``;
# Glue: hex UUID); no-SR serdes report a sorted tuple of field
# names. The :class:`MessagesBuffer` keys on this opaque value.
SchemaIdSpec: TypeAlias = Union[str, tuple[str, ...], dict[str, str]]

SchemaIdCompatibles: TypeAlias = Callable[[SchemaIdSpec, SchemaIdSpec], bool]


class KafkaSerde(ABC):
    """Abstract Kafka (de)serialiser. Subclasses set up the
    ``serializer`` / ``deserializer`` callables in their
    ``__init__`` and provide a schema-id extractor + compat
    checker for the message buffer."""

    def __init__(self) -> None:
        def _deserializer_with_schema_id(
            data: bytes,
            ctx: "SerializationContext",
        ) -> tuple[dict, SchemaIdSpec]:
            message = self.deserializer()(data, ctx)
            schema_id = self.extract_schema_id(data, message)
            return message, schema_id

        self._deserializer_with_schema_id = _deserializer_with_schema_id

    @property
    @abstractmethod
    def format(self) -> MsgFormatSpec: ...

    @abstractmethod
    def serializer(
        self,
    ) -> Callable[[dict, "SerializationContext"], bytes]: ...

    @abstractmethod
    def deserializer(
        self,
    ) -> Callable[[bytes, "SerializationContext"], dict]: ...

    @abstractmethod
    def extract_schema_id(
        self,
        data: bytes,
        message: dict,
    ) -> SchemaIdSpec: ...

    def deserializer_with_schema_id(
        self,
    ) -> Callable[
        [bytes, "SerializationContext"],
        tuple[dict, SchemaIdSpec],
    ]:
        """Convenience wrapper used by the consumer: returns a
        callable that yields ``(record, schema_id)`` in one call."""
        return self._deserializer_with_schema_id

    @abstractmethod
    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles: ...

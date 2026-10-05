#
# Copyright 2026 Tabsdata Inc.
#

"""Abstract base for Kafka (de)serialisers.

Every concrete serde implements:

* :meth:`format` -- ``"avro"`` / ``"json"`` / ``"protobuf"``.
* :meth:`serializer` -- callable that bytes-encodes a dict.
* :meth:`deserializer` -- callable that dict-decodes bytes.
* :meth:`extract_schema_id` -- the compatibility key the
  ``KafkaEncoder`` groups records on. SR-aware serdes pull this from
  the wire header; no-SR serdes derive it from the message's
  field set.
* :meth:`schema_id_compatibles_checker` -- equality / subset
  predicate over the schema-ids the encoder uses to decide
  whether a record can join the current file.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Callable, Literal, TypeAlias, Union

from tabsdatak.conn.kafka.error import KafkaErrorCode

if TYPE_CHECKING:  # pragma: no cover -- typing-only
    from confluent_kafka.serialization import SerializationContext


# Wire-format identifier the serdes report.
MsgFormatSpec: TypeAlias = Literal["avro", "json", "protobuf"]

# The schema id a serde reports per message. SR-aware serdes
# report a stringified registry id (Confluent: ``str(int)``;
# Glue: hex UUID); no-SR serdes report a sorted tuple of field
# names. The ``KafkaEncoder`` keys on this opaque value.
SchemaIdSpec: TypeAlias = Union[str, tuple[str, ...], dict[str, str]]

SchemaIdCompatibles: TypeAlias = Callable[[SchemaIdSpec, SchemaIdSpec], bool]


class DecodeErrorKind(Enum):
    RETRYABLE = "retryable"
    FORMAT = "format"


def retryable_error_types() -> tuple[type[BaseException], ...]:
    types: list[type[BaseException]] = [
        ConnectionError,
        TimeoutError,
    ]
    try:
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        types.append(SchemaRegistryError)
    except ImportError:
        pass
    try:
        from requests.exceptions import (
            ConnectionError as RequestsConnectionError,
            Timeout as RequestsTimeout,
        )

        types.extend([
            RequestsConnectionError,
            RequestsTimeout,
        ])
    except ImportError:
        pass
    try:
        from botocore.exceptions import (
            ClientError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        )

        types.extend([
            ClientError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ])
    except ImportError:
        pass
    return tuple(types)


def classify_decode_error(error: BaseException) -> DecodeErrorKind:
    retryable = retryable_error_types()
    cause: BaseException | None = error
    while cause is not None:
        if isinstance(cause, retryable):
            return DecodeErrorKind.RETRYABLE
        api = getattr(cause, "api_error", None)
        if api is KafkaErrorCode.KAFKA_18:
            return DecodeErrorKind.RETRYABLE
        cause = cause.__cause__ or cause.__context__
    return DecodeErrorKind.FORMAT


def build_json_validator(json_schema: dict):
    import jsonschema

    cls = jsonschema.validators.validator_for(json_schema)
    try:
        cls.check_schema(json_schema)
    except jsonschema.SchemaError as e:
        raise KafkaErrorCode.KAFKA_14.exception(error=e)
    return cls(json_schema)


class KafkaSerde(ABC):
    """Abstract Kafka (de)serialiser. Subclasses set up the
    ``serializer`` / ``deserializer`` callables in their
    ``__init__`` and provide a schema-id extractor + compat
    checker for the stream encoder."""

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

    def batch_decoder(self):
        """A callable decoding a whole fetched batch -- the payloads, in order --
        into one Arrow table, or returning None when that batch must be decoded
        message by message; None when this serde has no batch path."""
        return None

    @abstractmethod
    def schema_id_compatibles_checker(self) -> SchemaIdCompatibles: ...

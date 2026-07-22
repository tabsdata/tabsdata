#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass
from typing import Callable, Literal, TypeAlias, Union

from tabsdata._credentials import S3AccessKeyCredentials, UserPasswordCredentials

SchemaIdSpec: TypeAlias = Union[str, dict[str, str]]

SchemaIdCompatibles: TypeAlias = Callable[[SchemaIdSpec, SchemaIdSpec], bool]

KafkaServersSpec: TypeAlias = Union[str, list[str]]

AwsGlueSchemaRegistryCredentialsSpec: TypeAlias = S3AccessKeyCredentials

ConfluentSchemaRegistryCredentialsSpec: TypeAlias = UserPasswordCredentials

KafkaCredentialsSpec: TypeAlias = UserPasswordCredentials


@dataclass(frozen=True)
class Protobuf:
    """
    Represents the Protobuf message format configuration.

    This class is used to specify the details required for handling Protobuf
    messages, particularly the name of the message type to be used for
    deserialization.

    Attributes:
        message_name: The name of the Protobuf message type.
    """

    message_name: str


KafkaMessageFormatSpec: TypeAlias = Union[Literal["avro", "json"], Protobuf]

_KafkaMessageFormatNameSpec: TypeAlias = Literal["avro", "json", "protobuf"]

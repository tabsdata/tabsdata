#
# Copyright 2026 Tabsdata Inc.
#

import importlib.metadata

from tabsdata.connector.kafka._connector import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaConn,
    KafkaTrigger,
)
from tabsdata.connector.kafka.typing import (
    AwsGlueSchemaRegistryCredentialsSpec,
    ConfluentSchemaRegistryCredentialsSpec,
    KafkaCredentialsSpec,
    KafkaMessageFormatSpec,
    KafkaServersSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
)

__all__ = [
    "KafkaConn",
    "KafkaTrigger",
    "KafkaMessageFormatSpec",
    "SchemaIdCompatibles",
    "SchemaIdSpec",
    "KafkaServersSpec",
    "KafkaCredentialsSpec",
    "ConfluentSchemaRegistryCredentialsSpec",
    "AwsGlueSchemaRegistryCredentialsSpec",
    "ConfluentSchemaRegistry",
    "AwsGlueSchemaRegistry",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-kafka")
except Exception:
    __version__ = "unknown"

version = __version__

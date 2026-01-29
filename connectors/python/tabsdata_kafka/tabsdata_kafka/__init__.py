#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_kafka._connector import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaConn,
    KafkaTrigger,
)
from tabsdata_kafka.shared_types import (
    AwsGlueAccessKeyCredentials,
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
    "AwsGlueAccessKeyCredentials",
    "ConfluentSchemaRegistry",
    "AwsGlueSchemaRegistry",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_kafka")
except Exception:
    __version__ = "unknown"

version = __version__

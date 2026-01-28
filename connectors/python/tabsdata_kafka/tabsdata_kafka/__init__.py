#
# Copyright 2026 Tabs Data Inc.
#

import importlib.metadata

from tabsdata_kafka._connector import (
    KafkaConn,
    KafkaStageTrigger,
    ConfluentSchemaRegistry,
    AwsGlueSchemaRegistry,
)
from tabsdata_kafka.shared_types import (
    KafkaMessageFormatSpec,
    SchemaIdCompatibles,
    SchemaIdSpec,
    KafkaServersSpec,
    AwsGlueAccessKeyCredentials,
)
from tabsdata_kafka.consumer.rollovers import MessagesRollover, SizeRollover

__all__ = [
    "KafkaConn",
    "KafkaStageTrigger",
    "KafkaMessageFormatSpec",
    "SchemaIdCompatibles",
    "SchemaIdSpec",
    "KafkaServersSpec",
    "AwsGlueAccessKeyCredentials",
    "ConfluentSchemaRegistry",
    "AwsGlueSchemaRegistry",
    "MessagesRollover",
    "SizeRollover",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata_kafka")
except Exception:
    __version__ = "unknown"

version = __version__

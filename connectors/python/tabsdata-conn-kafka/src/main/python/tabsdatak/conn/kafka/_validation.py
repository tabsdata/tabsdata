#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.kafka.error import KafkaErrorCode

# --- Field-value rules -----------------------------------------------
#
# Kafka's alphabets are broad and the broker is the real authority on what
# it will accept, so these fields are gated only on being non-empty. The
# registry-facing specs (`ConfluentSchemaRegistryUrlNameSpec`,
# `RegistryNameSpec`, `SchemaNameSpec`) carry no rule at all.

SERVERS_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(KafkaErrorCode.KAFKA_21, min_len=1)
)

GROUP_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(KafkaErrorCode.KAFKA_22, min_len=1)
)

SCHEMA_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(KafkaErrorCode.KAFKA_23, min_len=1)
)

TOPIC_VALIDATION = _api.ValueConstraints(
    _api.Rule.length(KafkaErrorCode.KAFKA_24, min_len=1)
)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common import constants as _conn_constants
from tabsdatak.conn.kafka import constants as _constants

pytestmark = pytest.mark.kafka


def test_kafka_metadata_name():
    assert _constants.KAFKA_META_TOPIC == "@td.kafka.topic"


def test_kafka_metadata_name_carries_the_meta_column_prefix():
    assert _constants.KAFKA_META_TOPIC.startswith(_conn_constants.TD_META_COLUMN_PREFIX)

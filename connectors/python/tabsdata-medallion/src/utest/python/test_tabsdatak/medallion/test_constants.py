#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak._medallion import constants
from tabsdatak.conn.common.constants import TD_CONNECTOR_COLUMN_PREFIX
from tabsdatak.conn.common.sql.cdc.constants import DOT

pytestmark = pytest.mark.medallion

TD_MEDALLION_SYSTEM_COLUMN_SEGMENT = "medallion"

TD_OP_SYSTEM_COLUMN_SUFFIX = "op"
TD_PK_SYSTEM_COLUMN_SUFFIX = "pk"
TD_PERIOD_SYSTEM_COLUMN_SUFFIX = "period"
TD_SOURCE_TIMESTAMP_SYSTEM_COLUMN_SUFFIX = "source_ts"
TD_BRONZE_TIMESTAMP_SYSTEM_COLUMN_SUFFIX = "bronze_ts"
TD_SILVER_TIMESTAMP_SYSTEM_COLUMN_SUFFIX = "silver_ts"
TD_GOLD_TIMESTAMP_SYSTEM_COLUMN_SUFFIX = "gold_ts"
TD_VALID_FROM_SYSTEM_COLUMN_SUFFIX = "valid_from"
TD_VALID_TO_SYSTEM_COLUMN_SUFFIX = "valid_to"
TD_DATE_KEY_SYSTEM_COLUMN_SUFFIX = "date_sk"
TD_ENTITY_KEY_SYSTEM_COLUMN_SUFFIX = "entity_sk"
TD_VERSION_KEY_SYSTEM_COLUMN_SUFFIX = "version_sk"

_MEDALLION_PREFIX = f"{TD_CONNECTOR_COLUMN_PREFIX}{TD_MEDALLION_SYSTEM_COLUMN_SEGMENT}"

PREFIXES = [
    ("MEDALLION_SYSTEM_COLUMN_PREFIX", _MEDALLION_PREFIX),
]

NAMES = [
    (
        "MEDALLION_SYSTEM_COLUMN_OP",
        f"{_MEDALLION_PREFIX}{DOT}{TD_OP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_PK",
        f"{_MEDALLION_PREFIX}{DOT}{TD_PK_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_PERIOD",
        f"{_MEDALLION_PREFIX}{DOT}{TD_PERIOD_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_SOURCE_TS",
        f"{_MEDALLION_PREFIX}{DOT}{TD_SOURCE_TIMESTAMP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_BRONZE_TS",
        f"{_MEDALLION_PREFIX}{DOT}{TD_BRONZE_TIMESTAMP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_SILVER_TS",
        f"{_MEDALLION_PREFIX}{DOT}{TD_SILVER_TIMESTAMP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_GOLD_TS",
        f"{_MEDALLION_PREFIX}{DOT}{TD_GOLD_TIMESTAMP_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_VALID_FROM",
        f"{_MEDALLION_PREFIX}{DOT}{TD_VALID_FROM_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_VALID_TO",
        f"{_MEDALLION_PREFIX}{DOT}{TD_VALID_TO_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_DATE_SK",
        f"{_MEDALLION_PREFIX}{DOT}{TD_DATE_KEY_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_ENTITY_SK",
        f"{_MEDALLION_PREFIX}{DOT}{TD_ENTITY_KEY_SYSTEM_COLUMN_SUFFIX}",
    ),
    (
        "MEDALLION_SYSTEM_COLUMN_VERSION_SK",
        f"{_MEDALLION_PREFIX}{DOT}{TD_VERSION_KEY_SYSTEM_COLUMN_SUFFIX}",
    ),
]


@pytest.mark.parametrize("constant,composed", PREFIXES, ids=[p[0] for p in PREFIXES])
def test_prefix_matches_its_composition(constant, composed):
    assert getattr(constants, constant) == composed


@pytest.mark.parametrize("constant,composed", NAMES, ids=[n[0] for n in NAMES])
def test_name_matches_its_composition(constant, composed):
    assert getattr(constants, constant) == composed


def test_every_declared_prefix_is_covered():
    declared = {name for name in vars(constants) if name.endswith("_PREFIX")}
    assert declared == {constant for constant, _ in PREFIXES}


def test_every_declared_name_is_covered():
    declared = {
        name
        for name in vars(constants)
        if name.startswith("MEDALLION_SYSTEM_COLUMN") and not name.endswith("_PREFIX")
    }
    assert declared == {constant for constant, _ in NAMES}


def test_no_name_carries_a_repeated_qualifier():
    for _, composed in PREFIXES + NAMES:
        assert f"{DOT}{DOT}" not in composed

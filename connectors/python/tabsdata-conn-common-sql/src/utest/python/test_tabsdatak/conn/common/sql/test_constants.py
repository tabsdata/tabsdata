#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common import constants as _conn_constants
from tabsdatak.conn.common.sql import constants as _constants
from tabsdatak.conn.common.sql.cdc import constants as _cdc_constants


def test_sql_metadata_names():
    assert _constants.SQL_META_QUERY == "@td.sql.query"
    assert _constants.SQL_META_INITIAL_VALUES == "@td.sql.initial_values"


def test_sql_metadata_names_carry_the_connector_column_prefix():
    prefix = _conn_constants.TD_CONNECTOR_COLUMN_PREFIX
    for name in (_constants.SQL_META_QUERY, _constants.SQL_META_INITIAL_VALUES):
        assert name.startswith(prefix)


def test_cdc_names_share_the_connector_column_prefix():
    assert _cdc_constants.CDC_META_OP.startswith(
        _conn_constants.TD_CONNECTOR_COLUMN_PREFIX
    )


_DOT = _cdc_constants.DOT
_CDC_PREFIX = (
    f"{_conn_constants.TD_CONNECTOR_COLUMN_PREFIX}{_cdc_constants.TD_CDC_INFIX}"
)
_META_PREFIX = f"{_CDC_PREFIX}{_DOT}{_cdc_constants.TD_CDC_META_INFIX}"
_DATA_PREFIX = f"{_CDC_PREFIX}{_DOT}{_cdc_constants.TD_CDC_DATA_INFIX}"
_DATA_COL_PREFIX = f"{_DATA_PREFIX}{_DOT}{_cdc_constants.TD_CDC_COL_INFIX}"
_DATA_ROW_PREFIX = f"{_DATA_PREFIX}{_DOT}{_cdc_constants.TD_CDC_ROW_INFIX}"
_DATA_MAP_PREFIX = f"{_DATA_PREFIX}{_DOT}{_cdc_constants.TD_CDC_MAP_INFIX}"

CDC_PREFIXES = [
    ("CDC_META_PREFIX", _META_PREFIX),
    ("CDC_DATA_COL_PREFIX", _DATA_COL_PREFIX),
    (
        "CDC_DATA_COL_OLD_PREFIX",
        f"{_DATA_COL_PREFIX}{_DOT}{_cdc_constants.TD_CDC_OLD_INFIX}",
    ),
    (
        "CDC_DATA_COL_NEW_PREFIX",
        f"{_DATA_COL_PREFIX}{_DOT}{_cdc_constants.TD_CDC_NEW_INFIX}",
    ),
]

CDC_NAMES = [
    ("CDC_META_OP", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_OP_INFIX}"),
    ("CDC_META_TX", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_TX_INFIX}"),
    ("CDC_META_SQ", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_SQ_INFIX}"),
    ("CDC_META_TS", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_TS_INFIX}"),
    ("CDC_META_SEG", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_SEG_INFIX}"),
    ("CDC_META_ORD", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_ORD_INFIX}"),
    ("CDC_META_FMT", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_FMT_INFIX}"),
    ("CDC_META_FLAT", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_FLAT_INFIX}"),
    ("CDC_META_SCH", f"{_META_PREFIX}{_DOT}{_cdc_constants.TD_CDC_SCH_INFIX}"),
    (
        "CDC_DATA_ROW_OLD",
        f"{_DATA_ROW_PREFIX}{_DOT}{_cdc_constants.TD_CDC_OLD_INFIX}",
    ),
    (
        "CDC_DATA_ROW_NEW",
        f"{_DATA_ROW_PREFIX}{_DOT}{_cdc_constants.TD_CDC_NEW_INFIX}",
    ),
    (
        "CDC_DATA_MAP_OLD",
        f"{_DATA_MAP_PREFIX}{_DOT}{_cdc_constants.TD_CDC_OLD_INFIX}",
    ),
    (
        "CDC_DATA_MAP_NEW",
        f"{_DATA_MAP_PREFIX}{_DOT}{_cdc_constants.TD_CDC_NEW_INFIX}",
    ),
]


@pytest.mark.parametrize(
    "constant,composed", CDC_PREFIXES, ids=[p[0] for p in CDC_PREFIXES]
)
def test_cdc_prefix_matches_its_composition(constant, composed):
    assert getattr(_cdc_constants, constant) == composed


@pytest.mark.parametrize("constant,composed", CDC_NAMES, ids=[n[0] for n in CDC_NAMES])
def test_cdc_name_matches_its_composition(constant, composed):
    assert getattr(_cdc_constants, constant) == composed


def test_every_declared_cdc_prefix_is_covered():
    declared = {
        name
        for name in vars(_cdc_constants)
        if name.startswith("CDC_") and name.endswith("_PREFIX")
    }
    assert declared == {constant for constant, _ in CDC_PREFIXES}


def test_every_declared_cdc_name_is_covered():
    declared = {
        name
        for name in vars(_cdc_constants)
        if name.startswith(("CDC_META", "CDC_DATA")) and not name.endswith("_PREFIX")
    }
    assert declared == {constant for constant, _ in CDC_NAMES}


def test_no_cdc_name_carries_a_repeated_qualifier():
    for _, composed in CDC_PREFIXES + CDC_NAMES:
        assert f"{_DOT}{_DOT}" not in composed

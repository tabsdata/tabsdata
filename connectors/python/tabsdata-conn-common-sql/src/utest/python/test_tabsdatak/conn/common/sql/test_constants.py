#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

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
    # One definition of the prefix, owned by ``tabsdatak.conn.common``.
    assert (
        _cdc_constants.TD_CONNECTOR_COLUMN_PREFIX
        is _conn_constants.TD_CONNECTOR_COLUMN_PREFIX
    )
    assert _cdc_constants.CDC_META_OP.startswith(
        _conn_constants.TD_CONNECTOR_COLUMN_PREFIX
    )

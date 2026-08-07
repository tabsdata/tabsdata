#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common import constants as _constants


def test_connector_column_prefix():
    assert _constants.TD_CONNECTOR_COLUMN_PREFIX == "@td."

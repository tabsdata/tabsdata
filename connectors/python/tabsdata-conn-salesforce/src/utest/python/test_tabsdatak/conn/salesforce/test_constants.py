#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common import constants as _conn_constants
from tabsdatak.conn.salesforce import constants as _constants

pytestmark = pytest.mark.salesforce


def test_salesforce_metadata_names():
    assert _constants.SALESFORCE_META_SOQL == "@td.salesforce.soql"
    assert _constants.SALESFORCE_META_REPORT_ID == "@td.salesforce.reportId"
    assert _constants.SALESFORCE_META_LAST_MODIFIED == "@td.salesforce.last_modified"


def test_salesforce_metadata_names_carry_the_connector_column_prefix():
    for name in (
        _constants.SALESFORCE_META_SOQL,
        _constants.SALESFORCE_META_REPORT_ID,
        _constants.SALESFORCE_META_LAST_MODIFIED,
    ):
        assert name.startswith(_conn_constants.TD_CONNECTOR_COLUMN_PREFIX)

#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from test_tabsdata_cdc_postgresql.common.fixtures.docker import POSTGRESQL_CDC_DATABASE, POSTGRESQL_CDC_SLOT, POSTGRESQL_CDC_USER


@pytest.mark.cdc_postgresql
@pytest.mark.slow
def test_cdc_postgresql_fixture(postgresql_cdc_config):
    assert postgresql_cdc_config.port > 0
    assert postgresql_cdc_config.host == "127.0.0.1"
    assert postgresql_cdc_config.database == POSTGRESQL_CDC_DATABASE
    assert postgresql_cdc_config.cdc_user == POSTGRESQL_CDC_USER
    assert postgresql_cdc_config.slot == POSTGRESQL_CDC_SLOT

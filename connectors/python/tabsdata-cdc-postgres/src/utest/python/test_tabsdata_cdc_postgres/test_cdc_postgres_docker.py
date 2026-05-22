#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from test_tabsdata_cdc_postgres.common.fixtures.docker import POSTGRES_CDC_DATABASE, POSTGRES_CDC_SLOT, POSTGRES_CDC_USER

pytestmark = [pytest.mark.xdist_group("postgres_cdc_seq")]

@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_cdc_postgres_fixture(postgres_cdc_config):
    assert postgres_cdc_config.port > 0
    assert postgres_cdc_config.host == "127.0.0.1"
    assert postgres_cdc_config.database == POSTGRES_CDC_DATABASE
    assert postgres_cdc_config.cdc_user == POSTGRES_CDC_USER
    assert postgres_cdc_config.slot == POSTGRES_CDC_SLOT

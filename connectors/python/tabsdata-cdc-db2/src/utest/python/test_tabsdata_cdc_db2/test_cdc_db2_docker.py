#
# Copyright 2026 Tabs Data Inc.
#

import logging

import pytest

from test_tabsdata_cdc_db2.common.fixtures.docker import DB2_CDC_DATABASE

pytestmark = [pytest.mark.xdist_group("cdc-db2-cdc_db2_docker_seq")]

logger = logging.getLogger(__name__)


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_cdc_db2_fixture(db2_cdc_config):
    assert db2_cdc_config.port > 0
    assert db2_cdc_config.host == "127.0.0.1"
    assert db2_cdc_config.database == DB2_CDC_DATABASE

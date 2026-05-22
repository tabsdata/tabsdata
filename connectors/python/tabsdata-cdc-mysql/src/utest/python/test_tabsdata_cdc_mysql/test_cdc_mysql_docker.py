#
# Copyright 2026 Tabs Data Inc.
#

import pytest

from test_tabsdata_cdc_mysql.common.fixtures.docker import MYSQL_CDC_DATABASE, MYSQL_CDC_USER

pytestmark = [pytest.mark.xdist_group("cdc-mysql-cdc_mysql_docker_seq")]

@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_cdc_mysql_fixture(mysql_cdc_config):
    assert mysql_cdc_config.port > 0
    assert mysql_cdc_config.host == "127.0.0.1"
    assert mysql_cdc_config.database == MYSQL_CDC_DATABASE
    assert mysql_cdc_config.cdc_user == MYSQL_CDC_USER

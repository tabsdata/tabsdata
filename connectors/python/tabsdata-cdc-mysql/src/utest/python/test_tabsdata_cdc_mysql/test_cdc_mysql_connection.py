#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from test_tabsdata_cdc_mysql.common.fixtures.docker import (
    MYSQL_CDC_DATABASE,
)


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_cdc_mysql_connection(mysql_cdc_config):
    uri = f"mysql://{mysql_cdc_config.host}:{mysql_cdc_config.port}/{MYSQL_CDC_DATABASE}"
    credentials = UserPasswordCredentials(
        user=mysql_cdc_config.cdc_user,
        password=mysql_cdc_config.cdc_password,
    )
    conn = MySQLCdcConn(uri=uri, credentials=credentials)
    engine = conn._connect()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select 1"))
            assert result.scalar() == 1
    finally:
        engine.dispose()

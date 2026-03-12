#
# Copyright 2026 Tabs Data Inc.
#

import logging

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from test_tabsdata_cdc_db2.common.fixtures.docker import (
    DB2_CDC_DATABASE,
)

logger = logging.getLogger(__name__)


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_cdc_db2_connection(db2_cdc_config):
    uri = f"db2://{db2_cdc_config.host}:{db2_cdc_config.port}/{DB2_CDC_DATABASE}"
    credentials = UserPasswordCredentials(
        user=db2_cdc_config.cdc_user,
        password=db2_cdc_config.cdc_password,
    )
    conn = Db2CdcConn(uri=uri, credentials=credentials)
    engine = conn._connect()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select 1 from sysibm.sysdummy1"))
            assert result.scalar() == 1
    finally:
        engine.dispose()

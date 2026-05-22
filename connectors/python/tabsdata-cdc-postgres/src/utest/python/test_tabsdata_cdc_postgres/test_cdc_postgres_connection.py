#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from test_tabsdata_cdc_postgres.common.fixtures.docker import (
    POSTGRES_CDC_DATABASE,
)

pytestmark = [pytest.mark.xdist_group("postgres_cdc_seq")]

@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_cdc_postgres_connection(postgres_cdc_config):
    uri = f"postgresql://{postgres_cdc_config.host}:{postgres_cdc_config.port}/{POSTGRES_CDC_DATABASE}"
    credentials = UserPasswordCredentials(
        user=postgres_cdc_config.cdc_user,
        password=postgres_cdc_config.cdc_password,
    )
    conn = PostgresCdcConn(uri=uri, credentials=credentials)
    engine = conn._connect()
    try:
        with engine.connect() as connection:
            result = connection.execute(text("select 1"))
            assert result.scalar() == 1
    finally:
        engine.dispose()

#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn


@pytest.fixture
def mysql_collect_tables_setup(mysql_cdc_config):
    uri = (
        f"mysql://{mysql_cdc_config.host}:{mysql_cdc_config.port}"
        f"/{mysql_cdc_config.database}"
    )
    credentials = UserPasswordCredentials(
        user=mysql_cdc_config.cdc_user,
        password=mysql_cdc_config.cdc_password,
    )
    conn = MySQLCdcConn(uri=uri, credentials=credentials)
    session = conn._connect()
    try:
        yield conn, session, mysql_cdc_config
    finally:
        with session.connect() as connection:
            for table_name in ["td_ct_simple", "td_ct_extra", "td_ct_types"]:
                connection.execute(text(f"drop table if exists {table_name}"))
            connection.commit()
        session.dispose()

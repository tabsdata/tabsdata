#
# Copyright 2026 Tabs Data Inc.
#

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.db2._connection import Db2CdcConn


@pytest.fixture
def db2_collect_tables_setup(db2_cdc_config):
    uri = (
        f"db2://{db2_cdc_config.host}:{db2_cdc_config.port}"
        f"/{db2_cdc_config.database}"
    )
    credentials = UserPasswordCredentials(
        user=db2_cdc_config.cdc_user,
        password=db2_cdc_config.cdc_password,
    )
    conn = Db2CdcConn(uri=uri, credentials=credentials)
    session = conn._connect()
    try:
        yield conn, session, db2_cdc_config
    finally:
        for table_name in ["TD_CT_SIMPLE", "TD_CT_EXTRA", "TD_CT_TYPES"]:
            # noinspection PyBroadException
            try:
                with session.connect() as connection:
                    connection.execute(text(f'drop table "{table_name}"'))
                    connection.commit()
            except Exception:
                pass
        session.dispose()

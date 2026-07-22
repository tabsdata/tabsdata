#
# Copyright 2026 Tabsdata Inc.
#

import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn


@pytest.fixture
def postgres_collect_tables_setup(postgres_cdc_config):
    schema = postgres_cdc_config.cdc_user

    uri = (
        f"postgresql://{postgres_cdc_config.host}:{postgres_cdc_config.port}"
        f"/{postgres_cdc_config.database}"
    )
    credentials = UserPasswordCredentials(
        user=postgres_cdc_config.cdc_user,
        password=postgres_cdc_config.cdc_password,
    )
    conn = PostgresCdcConn(uri=uri, credentials=credentials)
    session = conn._connect()
    try:
        yield conn, session, postgres_cdc_config
    finally:
        with session.connect() as connection:
            for table_name in ["td_ct_simple", "td_ct_extra", "td_ct_types"]:
                connection.execute(text(
                    f"drop table if exists {schema}.{table_name}"
                ))
            connection.commit()
        session.dispose()

#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string

import docker
import pytest
from sqlalchemy import text

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from test_tabsdata_cdc_db2.common.fixtures.docker import (
    DB2_CDC_ASN_SCHEMA,
    DB2_CDC_STAGING_TABLE_PREFIX,
    DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME,
)

ASN_SCHEMA = DB2_CDC_ASN_SCHEMA
CD_PREFIX = DB2_CDC_STAGING_TABLE_PREFIX

TABLE_BASES = ["TD_CT_SIMPLE", "TD_CT_EXTRA", "TD_CT_TYPES", "TD_CT_NOPFX"]

logger = logging.getLogger(__name__)

def table_suffix(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase, k=length))


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
    container = docker.from_env().containers.get(
        DEFAULT_PYTEST_DB2_CDC_DOCKER_CONTAINER_NAME
    )
    suffix = table_suffix()
    tables: list[str] = []
    try:
        yield conn, session, db2_cdc_config, container, suffix, tables
    finally:
        for table in tables:
            # noinspection PyBroadException
            try:
                with session.connect() as connection:
                    logger.info(f"!drop table {ASN_SCHEMA}.{table}")
                    _statement = text(f'drop table "{table}"')
                    # connection.execute(_statement)
                    connection.commit()
            except Exception:
                pass
            # noinspection PyBroadException
            try:
                with session.connect() as connection:
                    cd_table = f"{CD_PREFIX}{table}"
                    logger.info(f"!drop table {ASN_SCHEMA}.{cd_table}")
                    _statement = text(f'drop table "{ASN_SCHEMA}"."{cd_table}"')
                    # connection.execute(_statement)
                    connection.commit()
            except Exception:
                pass
        session.dispose()

#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import mysql.connector
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._connector import MySQLCdcTrigger
from test_tabsdata_cdc_common.common.basic import run_continuous_dml_test
from test_tabsdata_cdc_mysql.common.db import server_id
from test_tabsdata_cdc_mysql.common.sql import MySQLCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_mysql
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_continuous_dml(mysql_cdc_config):
    config = mysql_cdc_config
    schema = config.database

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [
        TableObject(schema=schema, table=f"td_con_{suffix}_t{i}")
        for i in range(3)
    ]

    logger.debug(f"Tracking tables: {tables}")

    connection = mysql.connector.connect(
        host=config.host,
        port=config.port,
        database=schema,
        user=config.root_user,
        password=config.root_password,
        use_pure=True,
    )
    connection.autocommit = False

    cursor = connection.cursor()
    sampler = MySQLCdcSampler(config.container, config.port, cursor)

    for table in tables:
        sampler.create_table(table)
    connection.commit()

    uri = f"mysql://{config.host}:{config.port}/{schema}"
    credentials = UserPasswordCredentials(
        user=config.cdc_user,
        password=config.cdc_password,
    )
    conn = MySQLCdcConn(
        uri=uri,
        credentials=credentials,
    )

    trigger = MySQLCdcTrigger(
        conn=conn,
        tables=[t.qualified for t in tables],
        start_from="head",
        server_id=server_id(),
        blocking_timeout_seconds=3,
        buffer_max_rows=100,
        buffer_max_seconds=5,
        trigger_max_rows=200,
        trigger_max_seconds=5,
        end=datetime.now(timezone.utc) + timedelta(seconds=300),
    )

    run_continuous_dml_test(
        sampler=sampler,
        connection=connection,
        tables=tables,
        trigger=trigger,
    )

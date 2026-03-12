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
from test_tabsdata_cdc_common.common.basic import run_poll_changes_test
from test_tabsdata_cdc_mysql.common.db import server_id
from test_tabsdata_cdc_mysql.common.sql import MySQLCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_mysql
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_poll_changes_insert_update_delete(mysql_cdc_config):
    config = mysql_cdc_config
    schema = config.database

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_poll_{suffix}"
    table = TableObject(schema=schema, table=f"{name}")

    logger.debug(f"Tracking table: {table}")

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
        tables=[table.qualified],
        start_from="origin",
        server_id=server_id(),
        blocking_timeout_seconds=3,
        buffer_max_rows=10_000,
        buffer_max_seconds=10,
        trigger_max_seconds=10,
        end=datetime.now(timezone.utc) + timedelta(seconds=60),
    )

    run_poll_changes_test(
        connection=connection,
        table=table,
        sampler=sampler,
        trigger=trigger,
    )

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
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES
from test_tabsdata_cdc_common.common.continuous import (
    continuous_dml_params,
    run_continuous_dml_test,
)
from test_tabsdata_cdc_mysql.common.db import server_id
from test_tabsdata_cdc_mysql.common.sql import MySQLCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_mysql
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
@continuous_dml_params
def test_continuous_dml(mysql_cdc_config, q_tables, q_threads):
    config = mysql_cdc_config
    schema = config.database

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [
        TableObject(schema=schema, table=f"td_con_{suffix}_t{i}")
        for i in range(q_tables)
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
        tables=[table.qualified for table in tables],
        start_from="head",
        server_id=server_id(),
        blocking_timeout_seconds=4,
        buffer_max_rows=64,
        buffer_max_secs=4,
        trigger_max_rows=128,
        trigger_max_secs=4,
        end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
    )

    def dml_worker_factory():
        w_conn = mysql.connector.connect(
            host=config.host,
            port=config.port,
            database=schema,
            user=config.root_user,
            password=config.root_password,
            use_pure=True,
        )
        w_conn.autocommit = False
        w_cursor = w_conn.cursor()
        w_sampler = MySQLCdcSampler(config.container, config.port, w_cursor)
        return w_conn, w_sampler

    run_continuous_dml_test(
        sampler=sampler,
        connection=connection,
        tables=tables,
        trigger=trigger,
        dml_worker_threads=q_threads,
        dml_worker_factory=dml_worker_factory,
    )

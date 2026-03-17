#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest
from psycopg2.extras import LogicalReplicationConnection

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger
from test_tabsdata_cdc_common.common.basic import run_continuous_dml_test
from test_tabsdata_cdc_postgres.common.db import replication_slot
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_continuous_dml(postgres_cdc_config):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [
        TableObject(schema=schema, table=f"td_con_{suffix}_t{i}")
        for i in range(3)
    ]
    slot_name = replication_slot()

    logger.debug(f"Tracking tables: {tables}")
    logger.debug(f"Replication slot: {slot_name}")

    connection = psycopg2.connect(
        host=config.host,
        port=str(config.port),
        dbname=config.database,
        user=config.root_user,
        password=config.root_password,
    )
    connection.autocommit = False

    cursor = connection.cursor()
    sampler = PostgresCdcSampler(config.container, config.port, cursor)

    for table in tables:
        sampler.create_table(table)
    connection.commit()

    uri = f"postgresql://{config.host}:{config.port}/{config.database}"
    credentials = UserPasswordCredentials(
        user=config.cdc_user,
        password=config.cdc_password,
    )
    conn = PostgresCdcConn(
        uri=uri,
        credentials=credentials,
    )

    rconn = psycopg2.connect(
        host=config.host,
        port=str(config.port),
        dbname=config.database,
        user=config.cdc_user,
        password=config.cdc_password,
        connection_factory=LogicalReplicationConnection,
    )
    rcursor = rconn.cursor()
    rcursor.create_replication_slot(slot_name, output_plugin="wal2json")
    rcursor.close()
    rconn.close()

    trigger = PostgresCdcTrigger(
        conn=conn,
        tables=[t.qualified for t in tables],
        start_from="head",
        replication_slot=slot_name,
        slot_behavior="reuse",
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

#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES
from test_tabsdata_cdc_common.common.continuous import (
    continuous_dml_params_tables_4,
    run_continuous_dml_test,
)
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
@continuous_dml_params_tables_4
def test_continuous_dml(postgres_cdc_config, postgres_cdc_slot, q_tables, q_threads):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [
        TableObject(schema=schema, table=f"td_con_{suffix}_t{i}")
        for i in range(q_tables)
    ]
    logger.debug(f"Tracking tables: {tables}")
    logger.debug(f"Replication slot: {postgres_cdc_slot}")

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

    trigger = PostgresCdcTrigger(
        conn=conn,
        tables=[table.qualified for table in tables],
        start_from="head",
        replication_slot=postgres_cdc_slot,
        slot_behavior="reuse",
        blocking_timeout_seconds=4,
        buffer_max_rows=64,
        buffer_max_secs=4,
        trigger_max_rows=128,
        trigger_max_secs=4,
        end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
    )

    def dml_worker_factory():
        w_conn = psycopg2.connect(
            host=config.host,
            port=str(config.port),
            dbname=config.database,
            user=config.root_user,
            password=config.root_password,
        )
        w_conn.autocommit = False
        w_cursor = w_conn.cursor()
        w_sampler = PostgresCdcSampler(config.container, config.port, w_cursor)
        return w_conn, w_sampler

    run_continuous_dml_test(
        sampler=sampler,
        connection=connection,
        tables=tables,
        trigger=trigger,
        dml_worker_threads=q_threads,
        dml_worker_factory=dml_worker_factory,
    )

#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import ibm_db_dbi
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._connector import Db2CdcTrigger
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES
from test_tabsdata_cdc_common.common.continuous import (
    continuous_dml_params_tables_2,
    run_continuous_dml_test,
)
from test_tabsdata_cdc_db2.common.fixtures.docker import DB2_CDC_STAGING_TABLE_PREFIX
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = [pytest.mark.xdist_group("cdc-db2-cdc_db2_continuous_tables_2_seq")]

@pytest.mark.cdc_db2
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
@continuous_dml_params_tables_2
def test_continuous_dml(db2_cdc_config, q_tables, q_threads):
    config = db2_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    tables = [
        TableObject(schema=schema, table=f"td_con_{suffix}_t{i}")
        for i in range(q_tables)
    ]
    cd_tables = [
        TableObject(
            schema="asn",
            table=f"{DB2_CDC_STAGING_TABLE_PREFIX}{table.table}",
        )
        for table in tables
    ]

    logger.debug(f"Tracking tables: {tables}")
    logger.debug(f"Tracking cd tables: {cd_tables}")

    connection = ibm_db_dbi.connect(
        host=config.host,
        dsn=f"PORT={config.port};PROTOCOL=TCPIP;",
        database=config.database,
        user=config.root_user,
        password=config.root_password,
    )
    try:
        connection.autocommit = False

        cursor = connection.cursor()
        sampler = Db2CdcSampler(config.container, config.port, cursor)

        for table in tables:
            sampler.create_table(table)
        connection.commit()

        uri = f"db2://{config.host}:{config.port}/{config.database}"
        credentials = UserPasswordCredentials(
            user=config.cdc_user,
            password=config.cdc_password,
        )
        conn = Db2CdcConn(
            uri=uri,
            credentials=credentials,
        )

        trigger = Db2CdcTrigger(
            conn=conn,
            tables=[cd_table.qualified for cd_table in cd_tables],
            start_from="head",
            blocking_timeout_seconds=4,
            buffer_max_rows=64,
            buffer_max_secs=4,
            trigger_max_rows=128,
            trigger_max_secs=4,
            end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
        )

        def dml_worker_factory():
            w_conn = ibm_db_dbi.connect(
                host=config.host,
                dsn=f"PORT={config.port};PROTOCOL=TCPIP;",
                database=config.database,
                user=config.root_user,
                password=config.root_password,
            )
            w_conn.autocommit = False
            w_cursor = w_conn.cursor()
            w_sampler = Db2CdcSampler(config.container, config.port, w_cursor)
            return w_conn, w_sampler

        run_continuous_dml_test(
            sampler=sampler,
            connection=connection,
            tables=tables,
            trigger=trigger,
            dml_worker_threads=q_threads,
            dml_worker_factory=dml_worker_factory,
        )
    finally:
        connection.close()

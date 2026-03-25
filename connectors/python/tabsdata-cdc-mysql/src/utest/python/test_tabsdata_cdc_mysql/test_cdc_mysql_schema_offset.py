#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import mysql.connector
import polars as pl
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._connector import MySQLCdcTrigger
from tabsdata.connector.cdc.mysql.typing import GtidPosition
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES
from test_tabsdata_cdc_common.common.schema import run_schema_evolution_offset_test
from test_tabsdata_cdc_common.common.evolution import SCHEMA_CHANGE_PARAMS
from test_tabsdata_cdc_mysql.common.db import server_id
from test_tabsdata_cdc_mysql.common.sql import MySQLCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_mysql
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
@pytest.mark.parametrize("change_kinds", SCHEMA_CHANGE_PARAMS)
def test_schema_evolution_offset(mysql_cdc_config, change_kinds):
    config = mysql_cdc_config
    schema = config.database

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_sco_{suffix}"
    table = TableObject(schema=schema, table=name)

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

    def trigger_factory(
        iteration: int,
        checkpoint_df: pl.DataFrame | None,
    ) -> MySQLCdcTrigger:
        if iteration == 0 or checkpoint_df is None or checkpoint_df.is_empty():
            start_from = "head"
        else:
            gtid = checkpoint_df["gtid"][0]
            start_from = GtidPosition(gtid=gtid)
        return MySQLCdcTrigger(
            conn=conn,
            tables=[table.qualified],
            start_from=start_from,
            server_id=server_id(),
            blocking_timeout_seconds=3,
            buffer_max_rows=5,
            buffer_max_bytes=None,
            buffer_max_secs=10,
            trigger_max_rows=7,
            trigger_max_bytes=None,
            trigger_max_secs=10,
            end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
        )

    run_schema_evolution_offset_test(
        sampler=sampler,
        connection=connection,
        table=table,
        trigger_factory=trigger_factory,
        change_kinds=change_kinds,
    )

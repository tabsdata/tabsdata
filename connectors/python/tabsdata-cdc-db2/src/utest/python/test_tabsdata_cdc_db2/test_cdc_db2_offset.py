#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import ibm_db_dbi
import polars as pl
import pytest

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._connector import Db2CdcTrigger
from tabsdata.connector.cdc.db2.typing import TableCommitSeqPosition
from test_tabsdata_cdc_common.common.shared import TRIGGER_END_MINUTES, run_offset_resume_test
from test_tabsdata_cdc_db2.common.fixtures.docker import DB2_CDC_STAGING_TABLE_PREFIX
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_db2
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_offset_resume(db2_cdc_config):
    config = db2_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_off_{suffix}"
    table = TableObject(schema=schema, table=name)
    cd_table = TableObject(schema="asn", table=f"{DB2_CDC_STAGING_TABLE_PREFIX}{name}")

    logger.debug(f"Tracking table: {table}")
    logger.debug(f"Tracking cd table: {cd_table}")

    connection = ibm_db_dbi.connect(
        host=config.host,
        dsn=f"PORT={config.port};PROTOCOL=TCPIP;",
        database=config.database,
        user=config.root_user,
        password=config.root_password,
    )
    connection.autocommit = False

    cursor = connection.cursor()
    sampler = Db2CdcSampler(config.container, config.port, cursor)

    uri = f"db2://{config.host}:{config.port}/{config.database}"
    credentials = UserPasswordCredentials(
        user=config.cdc_user,
        password=config.cdc_password,
    )
    conn = Db2CdcConn(
        uri=uri,
        credentials=credentials,
    )

    def trigger_factory(
        iteration: int,
        checkpoint_df: pl.DataFrame | None,
    ) -> Db2CdcTrigger:
        if iteration == 0 or checkpoint_df is None or checkpoint_df.is_empty():
            start_from = "head"
        else:
            row = checkpoint_df.row(0, named=True)
            seqs = {col: row[col].hex() for col in checkpoint_df.columns}
            start_from = TableCommitSeqPosition(seqs=seqs)
        return Db2CdcTrigger(
            conn=conn,
            tables=[cd_table.qualified],
            start_from=start_from,
            blocking_timeout_seconds=6,
            buffer_max_rows=5,
            buffer_max_bytes=None,
            buffer_max_secs=10,
            trigger_max_rows=7,
            trigger_max_bytes=None,
            trigger_max_secs=10,
            end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
        )

    run_offset_resume_test(
        connection=connection,
        table=table,
        sampler=sampler,
        trigger_factory=trigger_factory,
    )

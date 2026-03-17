#
# Copyright 2026 Tabs Data Inc.
#

import logging
import random
import string
from datetime import datetime, timedelta, timezone

import polars as pl
import psycopg2
import pytest
from psycopg2.extras import LogicalReplicationConnection

from tabsdata._credentials import UserPasswordCredentials
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger
from tabsdata.connector.cdc.postgres.typing import LsnPosition
from test_tabsdata_cdc_common.common.basic import TRIGGER_END_MINUTES, run_offset_resume_test
from test_tabsdata_cdc_postgres.common.db import replication_slot
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_offset_resume(postgres_cdc_config):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_off_{suffix}"
    table = TableObject(schema=schema, table=f"{name}")
    slot_name = replication_slot()

    logger.debug(f"Tracking table: {table}")
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

    def query_slot_lsn() -> int:
        cursor.execute(
            "select confirmed_flush_lsn - '0/0'::pg_lsn "
            "from pg_replication_slots "
            f"where slot_name = '{slot_name}'"
        )
        row = cursor.fetchone()
        assert row is not None, f"Replication slot '{slot_name}' not found"
        return int(row[0])

    initial_lsn = query_slot_lsn()
    logger.debug(f"Slot '{slot_name}' initial confirmed_flush_lsn: {initial_lsn}")

    def trigger_factory(
        iteration: int,
        checkpoint_df: pl.DataFrame | None,
    ) -> PostgresCdcTrigger:
        if iteration == 0 or checkpoint_df is None or checkpoint_df.is_empty():
            start_from = "head"
        else:
            lsn = int(checkpoint_df["lsn"][0])
            start_from = LsnPosition(lsn=lsn)
        return PostgresCdcTrigger(
            conn=conn,
            tables=[table.qualified],
            start_from=start_from,
            replication_slot=slot_name,
            slot_behavior="reuse",
            blocking_timeout_seconds=3,
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

    final_lsn = query_slot_lsn()
    logger.debug(f"Slot '{slot_name}' final confirmed_flush_lsn: {final_lsn}")
    assert final_lsn > initial_lsn, (
        f"Slot '{slot_name}' confirmed_flush_lsn did not advance: "
        f"initial={initial_lsn}, final={final_lsn}"
    )

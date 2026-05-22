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
from test_tabsdata_cdc_common.common.schema import run_schema_evolution_test
from test_tabsdata_cdc_common.common.evolution import SINGLE_CHANGES
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler

pytestmark = [pytest.mark.xdist_group("postgres_cdc_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
@pytest.mark.parametrize("change_kinds", SINGLE_CHANGES)
def test_schema_evolution(postgres_cdc_config, postgres_cdc_slot, change_kinds):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_sch_{suffix}"
    table = TableObject(schema=schema, table=name)

    logger.debug(f"Tracking table: {table}")

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
    trigger = PostgresCdcTrigger(
        conn=conn,
        tables=[table.qualified],
        start_from="head",
        replication_slot=postgres_cdc_slot,
        slot_behavior="reuse",
        blocking_timeout_seconds=3,
        buffer_max_rows=10_000,
        buffer_max_secs=10,
        trigger_max_secs=10,
        end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
    )

    run_schema_evolution_test(
        sampler=sampler,
        connection=connection,
        table=table,
        trigger=trigger,
        change_kinds=change_kinds,
    )

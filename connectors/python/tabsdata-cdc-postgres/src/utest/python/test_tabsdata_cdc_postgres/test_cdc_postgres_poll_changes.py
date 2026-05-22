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
from test_tabsdata_cdc_common.common.shared import (
    TRIGGER_END_MINUTES,
    run_end_to_end_test,
    run_poll_changes_test,
)
from test_tabsdata_cdc_common.common.format import (
    CDC_FORMAT_PARAMS,
    run_end_to_end_format_test,
)
from test_tabsdata_cdc_postgres.common.sql import PostgresCdcSampler

pytestmark = [pytest.mark.xdist_group("postgres_cdc_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_poll_changes_insert_update_delete(postgres_cdc_config, postgres_cdc_slot):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_poll_{suffix}"
    table = TableObject(schema=schema, table=f"{name}")

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

    run_poll_changes_test(
        connection=connection,
        table=table,
        sampler=sampler,
        trigger=trigger,
    )


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfnjob(venv=False)
def test_end_to_end_insert_update_delete(postgres_cdc_config, postgres_cdc_slot):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_e2e_{suffix}"
    table = TableObject(schema=schema, table=f"{name}")

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

    run_end_to_end_test(
        connection=connection,
        table=table,
        sampler=sampler,
        trigger=trigger,
    )


@pytest.mark.cdc_postgres
@pytest.mark.slow
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize("cdc_format", CDC_FORMAT_PARAMS)
def test_end_to_end_format(postgres_cdc_config, postgres_cdc_slot, cdc_format):
    config = postgres_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_fmt_{suffix}"
    table = TableObject(schema=schema, table=f"{name}")

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
        cdc_format=cdc_format,
        blocking_timeout_seconds=3,
        buffer_max_rows=10_000,
        buffer_max_secs=10,
        trigger_max_secs=10,
        end=datetime.now(timezone.utc) + timedelta(minutes=TRIGGER_END_MINUTES),
    )

    run_end_to_end_format_test(
        connection=connection,
        table=table,
        sampler=sampler,
        trigger=trigger,
        cdc_format=cdc_format,
    )

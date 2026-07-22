#
# Copyright 2026 Tabsdata Inc.
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
from test_tabsdata_cdc_common.common.shared import (
    TRIGGER_END_MINUTES,
    run_end_to_end_test,
    run_poll_changes_test,
)
from test_tabsdata_cdc_common.common.format import (
    CDC_FORMAT_PARAMS,
    run_end_to_end_format_test,
)
from test_tabsdata_cdc_db2.common.fixtures.docker import DB2_CDC_STAGING_TABLE_PREFIX
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler

pytestmark = [pytest.mark.xdist_group("cdc-db2-cdc_db2_poll_changes_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.cdc_db2
@pytest.mark.slow
@pytest.mark.tdfntrigger(venv=False)
def test_poll_changes_insert_update_delete(db2_cdc_config):
    config = db2_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_poll_{suffix}"
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
    try:
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
        trigger = Db2CdcTrigger(
            conn=conn,
            tables=[cd_table.qualified],
            start_from="head",
            blocking_timeout_seconds=6,
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
    finally:
        connection.close()


@pytest.mark.cdc_db2
@pytest.mark.slow
@pytest.mark.tdfnjob(venv=False)
def test_end_to_end_insert_update_delete(db2_cdc_config):
    config = db2_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_e2e_{suffix}"
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
    try:
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
        trigger = Db2CdcTrigger(
            conn=conn,
            tables=[cd_table.qualified],
            start_from="head",
            blocking_timeout_seconds=6,
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
    finally:
        connection.close()


@pytest.mark.cdc_db2
@pytest.mark.slow
@pytest.mark.tdfnjob(venv=False)
@pytest.mark.parametrize("cdc_format", CDC_FORMAT_PARAMS)
def test_end_to_end_format(db2_cdc_config, cdc_format):
    config = db2_cdc_config
    schema = config.cdc_user

    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=12))
    name = f"td_fmt_{suffix}"
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
    try:
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
        trigger = Db2CdcTrigger(
            conn=conn,
            tables=[cd_table.qualified],
            start_from="head",
            cdc_format=cdc_format,
            blocking_timeout_seconds=6,
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
    finally:
        connection.close()

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import random
import string

import mysql.connector
import pytest

from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import CdcOperation
from test_tabsdata.test_tableframe.common import pretty_polars, pretty_pandas
from test_tabsdata_cdc_common.common.sql import COLUMNS
from test_tabsdata_cdc_mysql.common.sql import MySQLCdcSampler


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_sampler_insert_update_delete(mysql_cdc_config):
    config = mysql_cdc_config
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    table = TableObject(schema=config.database, table=f"td_sampler_{suffix}")

    connection = mysql.connector.connect(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.root_user,
        password=config.root_password,
        use_pure=True,
    )
    try:
        cursor = connection.cursor()
        sampler = MySQLCdcSampler(cursor)

        sampler.run(
            container=mysql_cdc_config.container,
            port=mysql_cdc_config.port,
            table=table,
            inserts=8,
            updates=4,
            deletes=2)
        connection.commit()

        data = sampler.t_data(table)
        changes = sampler.t_changes(table)

        assert data.height == 6, f"Expected 6 rows, got {data.height}"

        assert changes.height == 14, f"Expected 14 changes, got {changes.height}"

        ops = changes["op"].to_list()
        assert ops.count(CdcOperation.INSERT) == 8
        assert ops.count(CdcOperation.UPDATE) == 4
        assert ops.count(CdcOperation.DELETE) == 2

        for column in COLUMNS:
            assert column in data.columns, f"Missing column {column} in data"

        sampler.run(
            container=mysql_cdc_config.container,
            port=mysql_cdc_config.port,
            table=table,
            inserts=4,
            updates=2,
            deletes=1)
        connection.commit()

        data = sampler.t_data(table)
        changes = sampler.t_changes(table)

        assert data.height == 9, f"Expected 9 rows, got {data.height}"

        assert changes.height == 21, f"Expected 21 changes, got {changes.height}"

        cursor.close()

        pretty_polars()
        pretty_pandas()

        df_data = sampler.t_data(table)
        print(df_data)

        df_changes = sampler.t_changes(table)
        print(df_changes)
    finally:
        # cleanup = connection.cursor()
        # cleanup.execute(f"drop table if exists {table.qualified}")
        # connection.commit()
        # cleanup.close()
        connection.close()

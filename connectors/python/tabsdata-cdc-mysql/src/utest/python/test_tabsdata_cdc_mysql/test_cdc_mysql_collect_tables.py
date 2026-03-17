#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import pyarrow as pa
from sqlalchemy import text

from tabsdata.connector.cdc.mysql._ingester.binlog import BinlogIngester
from tabsdata.exceptions import CdcError, ErrorCode
from test_tabsdata_cdc_mysql.common.db import server_id


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_collect_tables_success(mysql_collect_tables_setup):
    conn, session, config = mysql_collect_tables_setup

    schema = config.database

    with session.connect() as connection:
        connection.execute(text(
            "create table td_ct_simple ("
            "id int primary key, "
            "name varchar(100)"
            ")"
        ))
        connection.execute(text(
            "create table td_ct_extra ("
            "id int primary key, "
            "value float"
            ")"
        ))
        connection.commit()

    ingester = BinlogIngester(
        conn=conn,
        tables=[f"`{schema}`.`td_ct_simple`", f"{schema}.td_ct_extra"],
        start_from="head",
        server_id=server_id(),
    )
    result = ingester.collect_tables(session)

    assert len(result) == 2

    key_simple = f"{schema}.td_ct_simple"
    key_extra = f"{schema}.td_ct_extra"
    assert key_simple in result
    assert key_extra in result

    simple = result[key_simple]
    assert simple.data_table.schema == schema
    assert simple.data_table.table == "td_ct_simple"
    col_names = {c.name for c in simple.columns}
    assert col_names == {"id", "name"}
    assert "id" in simple.pk

    extra = result[key_extra]
    assert extra.data_table.schema == schema
    assert extra.data_table.table == "td_ct_extra"
    col_names = {c.name for c in extra.columns}
    assert col_names == {"id", "value"}
    assert "id" in extra.pk


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_collect_tables_not_found(mysql_collect_tables_setup):
    conn, session, config = mysql_collect_tables_setup

    schema = config.database

    with session.connect() as connection:
        connection.execute(text(
            "create table td_ct_simple ("
            "id int primary key, "
            "name varchar(100)"
            ")"
        ))
        connection.commit()

    ingester = BinlogIngester(
        conn=conn,
        tables=[f"{schema}.td_ct_simple", f"{schema}.td_ct_nonexistent"],
        start_from="head",
        server_id=server_id(),
    )
    with pytest.raises(CdcError) as exc_info:
        ingester.collect_tables(session)
    assert exc_info.value.error_code == ErrorCode.CDC2


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_collect_tables_invalid_spec(mysql_collect_tables_setup):
    conn, session, config = mysql_collect_tables_setup

    ingester = BinlogIngester(
        conn=conn,
        tables=["no_dot_here"],
        start_from="head",
        server_id=server_id(),
    )
    with pytest.raises(CdcError) as exception:
        ingester.collect_tables(session)
    assert exception.value.error_code == ErrorCode.CDC1


@pytest.mark.cdc_mysql
@pytest.mark.slow
def test_collect_tables_type_conversion(mysql_collect_tables_setup):
    conn, session, config = mysql_collect_tables_setup

    schema = config.database

    with session.connect() as c:
        c.execute(text(
            "create table td_ct_types ("
            "col_smallint smallint, "
            "col_int int primary key, "
            "col_bigint bigint, "
            "col_float float, "
            "col_double double, "
            "col_decimal decimal(10,2), "
            "col_bool boolean, "
            "col_varchar varchar(255), "
            "col_date date, "
            "col_datetime datetime, "
            "col_time time, "
            "col_json json"
            ")"
        ))
        c.commit()

    ingester = BinlogIngester(
        conn=conn,
        tables=[f"{schema}.td_ct_types"],
        start_from="head",
        server_id=server_id(),
    )
    result = ingester.collect_tables(session)

    key = f"{schema}.td_ct_types"
    assert key in result

    table = result[key]
    cols = {c.name: c for c in table.columns}

    assert cols["col_smallint"].patype == pa.int16()
    assert cols["col_int"].patype == pa.int32()
    assert cols["col_bigint"].patype == pa.int64()
    assert cols["col_float"].patype == pa.float64()
    assert cols["col_double"].patype == pa.float64()
    assert cols["col_decimal"].patype == pa.decimal128(10, 2)
    assert cols["col_bool"].patype == pa.int32()
    assert cols["col_varchar"].patype == pa.utf8()
    assert cols["col_date"].patype == pa.date32()
    assert cols["col_datetime"].patype == pa.timestamp("us")
    assert cols["col_time"].patype == pa.time64("us")
    assert cols["col_json"].patype == pa.utf8()

    assert "col_int" in table.pk

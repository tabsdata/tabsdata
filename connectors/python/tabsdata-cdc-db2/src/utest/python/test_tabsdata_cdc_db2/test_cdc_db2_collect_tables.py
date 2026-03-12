#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import pyarrow as pa
from sqlalchemy import text

from tabsdata.connector.cdc.db2._ingester.sqlreplica import SqlReplicationIngester
from tabsdata.exceptions import CdcError, ErrorCode


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_success(db2_collect_tables_setup):
    conn, session, config = db2_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as connection:
        connection.execute(text(
            "create table td_ct_simple ("
            "id integer not null primary key, "
            "name varchar(100)"
            ")"
        ))
        connection.execute(text(
            "create table td_ct_extra ("
            "id integer not null primary key, "
            "value double"
            ")"
        ))
        connection.commit()

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[
            f'"{schema.upper()}"."TD_CT_SIMPLE"',
            f"{schema}.td_ct_extra",
        ],
    )
    result = ingester.collect_tables(session)

    assert len(result) == 2

    key_simple = f"{schema}.td_ct_simple"
    key_extra = f"{schema}.td_ct_extra"
    assert key_simple in result
    assert key_extra in result

    simple = result[key_simple]
    col_names = {c.name.lower() for c in simple.columns}
    assert col_names == {"id", "name"}
    pk_names = {p.lower() for p in simple.pk}
    assert "id" in pk_names

    extra = result[key_extra]
    col_names = {c.name.lower() for c in extra.columns}
    assert col_names == {"id", "value"}
    pk_names = {p.lower() for p in extra.pk}
    assert "id" in pk_names


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_not_found(db2_collect_tables_setup):
    conn, session, config = db2_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as connection:
        connection.execute(text(
            "create table td_ct_simple ("
            "id integer not null primary key, "
            "name varchar(100)"
            ")"
        ))
        connection.commit()

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[
            f"{schema}.td_ct_simple",
            f"{schema}.td_ct_nonexistent",
        ],
    )
    with pytest.raises(CdcError) as exc_info:
        ingester.collect_tables(session)
    assert exc_info.value.error_code == ErrorCode.CDC2


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_invalid_spec(db2_collect_tables_setup):
    conn, session, config = db2_collect_tables_setup

    ingester = SqlReplicationIngester(conn=conn, tables=["no_dot_here"])
    with pytest.raises(CdcError) as exc_info:
        ingester.collect_tables(session)
    assert exc_info.value.error_code == ErrorCode.CDC1


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_type_conversion(db2_collect_tables_setup):
    conn, session, config = db2_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as c:
        c.execute(text(
            "create table td_ct_types ("
            "col_smallint smallint, "
            "col_int integer not null primary key, "
            "col_bigint bigint, "
            "col_real real, "
            "col_double double, "
            "col_decimal decimal(10,2), "
            "col_varchar varchar(255), "
            "col_date date, "
            "col_timestamp timestamp, "
            "col_time time, "
            "col_blob blob(1024)"
            ")"
        ))
        c.commit()

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[f"{schema}.td_ct_types"],
    )
    result = ingester.collect_tables(session)

    key = f"{schema}.td_ct_types"
    assert key in result

    table = result[key]
    cols = {c.name.lower(): c for c in table.columns}

    assert cols["col_smallint"].patype == pa.int16()
    assert cols["col_int"].patype == pa.int32()
    assert cols["col_bigint"].patype == pa.int64()
    assert cols["col_real"].patype == pa.float64()
    assert cols["col_double"].patype == pa.float64()
    assert cols["col_decimal"].patype == pa.decimal128(10, 2)
    assert cols["col_varchar"].patype == pa.utf8()
    assert cols["col_date"].patype == pa.date32()
    assert cols["col_timestamp"].patype == pa.timestamp("us")
    assert cols["col_time"].patype == pa.time64("us")
    assert cols["col_blob"].patype == pa.binary()

    pk_names = {p.lower() for p in table.pk}
    assert "col_int" in pk_names

#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import pyarrow as pa
from sqlalchemy import text

from tabsdata.connector.cdc.postgres._ingester.wal2json import Wal2JsonIngester
from tabsdata.exceptions import CdcError, ErrorCode
from test_tabsdata_cdc_postgres.common.db import replication_slot


@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_collect_tables_success(postgres_collect_tables_setup):
    conn, session, config = postgres_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as connection:
        connection.execute(text(
            f"create table {schema}.td_ct_simple ("
            "id integer primary key, "
            "name varchar(100)"
            ")"
        ))
        connection.execute(text(
            f"create table {schema}.td_ct_extra ("
            "id integer primary key, "
            "value float"
            ")"
        ))
        connection.commit()

    ingester = Wal2JsonIngester(
        conn=conn,
        tables=[f'"{schema}"."td_ct_simple"', f"{schema}.td_ct_extra"],
        start_from="origin",
        replication_slot=replication_slot(),
        replication_slot_behavior="create",
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


@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_collect_tables_not_found(postgres_collect_tables_setup):
    conn, session, config = postgres_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as c:
        c.execute(text(
            f"create table {schema}.td_ct_simple ("
            "id integer primary key, "
            "name varchar(100)"
            ")"
        ))
        c.commit()

    ingester = Wal2JsonIngester(
        conn=conn,
        tables=[f"{schema}.td_ct_simple", f"{schema}.td_ct_nonexistent"],
        start_from="origin",
        replication_slot=replication_slot(),
        replication_slot_behavior="create",
    )
    with pytest.raises(CdcError) as exc_info:
        ingester.collect_tables(session)
    assert exc_info.value.error_code == ErrorCode.CDC2


@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_collect_tables_invalid_spec(postgres_collect_tables_setup):
    conn, session, config = postgres_collect_tables_setup

    ingester = Wal2JsonIngester(
        conn=conn,
        tables=["no_dot_here"],
        start_from="origin",
        replication_slot=replication_slot(),
        replication_slot_behavior="create",
    )
    with pytest.raises(CdcError) as exception:
        ingester.collect_tables(session)
    assert exception.value.error_code == ErrorCode.CDC1


@pytest.mark.cdc_postgres
@pytest.mark.slow
def test_collect_tables_type_conversion(postgres_collect_tables_setup):
    conn, session, config = postgres_collect_tables_setup

    schema = config.cdc_user

    with session.connect() as c:
        c.execute(text(
            f"create table {schema}.td_ct_types ("
            "col_smallint smallint, "
            "col_int integer primary key, "
            "col_bigint bigint, "
            "col_real real, "
            "col_double double precision, "
            "col_numeric numeric(10,2), "
            "col_bool boolean, "
            "col_varchar varchar(255), "
            "col_text text, "
            "col_date date, "
            "col_timestamp timestamp, "
            "col_timestamptz timestamptz, "
            "col_time time, "
            "col_bytea bytea, "
            "col_json json, "
            "col_jsonb jsonb, "
            "col_uuid uuid"
            ")"
        ))
        c.commit()

    ingester = Wal2JsonIngester(
        conn=conn,
        tables=[f"{schema}.td_ct_types"],
        start_from="origin",
        replication_slot=replication_slot(),
        replication_slot_behavior="create",
    )
    result = ingester.collect_tables(session)

    key = f"{schema}.td_ct_types"
    assert key in result

    table = result[key]
    cols = {c.name: c for c in table.columns}

    assert cols["col_smallint"].patype == pa.int16()
    assert cols["col_int"].patype == pa.int32()
    assert cols["col_bigint"].patype == pa.int64()
    assert cols["col_real"].patype == pa.float64()
    assert cols["col_double"].patype == pa.float64()
    assert cols["col_numeric"].patype == pa.decimal128(10, 2)
    assert cols["col_bool"].patype == pa.bool_()
    assert cols["col_varchar"].patype == pa.utf8()
    assert cols["col_text"].patype == pa.utf8()
    assert cols["col_date"].patype == pa.date32()
    assert cols["col_timestamp"].patype == pa.timestamp("us")
    assert cols["col_timestamptz"].patype == pa.timestamp("us", tz="UTC")
    assert cols["col_time"].patype == pa.time64("us")
    assert cols["col_bytea"].patype == pa.binary()
    assert cols["col_json"].patype == pa.utf8()
    assert cols["col_jsonb"].patype == pa.utf8()
    assert cols["col_uuid"].patype == pa.utf8()

    assert "col_int" in table.pk

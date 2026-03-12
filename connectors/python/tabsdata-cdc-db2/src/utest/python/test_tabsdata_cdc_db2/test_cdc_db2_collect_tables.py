#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import pyarrow as pa

from tabsdata.connector.cdc.db2._ingester.sqlreplica import SqlReplicationIngester
from tabsdata.exceptions import CdcError, ErrorCode
from test_tabsdata_cdc_db2.common.fixtures.docker import (
    DB2_CDC_OLD_VALUES_PREFIX,
    register_table_for_cdc,
)
from test_tabsdata_cdc_db2.common.fixtures.schema import (
    ASN_SCHEMA,
    CD_PREFIX,
)

OLD_VALUES_PREFIX = DB2_CDC_OLD_VALUES_PREFIX


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_success(db2_collect_tables_setup):
    conn, session, config, container, suffix, tables = db2_collect_tables_setup

    schema = config.cdc_user

    simple = f"TD_CT_SIMPLE_{suffix}"
    extra = f"TD_CT_EXTRA_{suffix}"
    tables.extend([simple, extra])

    register_table_for_cdc(
        container,
        config.port,
        schema=schema,
        table=simple,
        ddl=(
            f"create table {simple} ("
            "id integer not null primary key, "
            "name varchar(100)"
            ")"
        ),
        wait=False,
    )
    register_table_for_cdc(
        container,
        config.port,
        schema=schema,
        table=extra,
        ddl=(
            f"create table {extra} ("
            "id integer not null primary key, "
            "value double"
            ")"
        ),
        wait=False,
    )

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[
            f"{ASN_SCHEMA}.{CD_PREFIX}{simple}",
            f"{ASN_SCHEMA}.{CD_PREFIX}{extra}",
        ],
    )
    result = ingester.collect_tables(session)

    assert len(result) == 2

    key_simple = f"{schema}.{simple.lower()}"
    key_extra = f"{schema}.{extra.lower()}"
    assert key_simple in result
    assert key_extra in result

    simple_t = result[key_simple]
    assert simple_t.data_table.qualified == key_simple
    assert simple_t.cdc_table.qualified != simple_t.data_table.qualified
    col_names = {c.name.lower() for c in simple_t.columns}
    assert col_names == {"id", "name"}
    for col in simple_t.columns:
        assert col.iname == f"{OLD_VALUES_PREFIX}{col.name}"
    pk_names = {p.lower() for p in simple_t.pk}
    assert "id" in pk_names

    extra_t = result[key_extra]
    assert extra_t.data_table.qualified == key_extra
    assert extra_t.cdc_table.qualified != extra_t.data_table.qualified
    col_names = {c.name.lower() for c in extra_t.columns}
    assert col_names == {"id", "value"}
    for col in extra_t.columns:
        assert col.iname == f"{OLD_VALUES_PREFIX}{col.name}"
    pk_names = {p.lower() for p in extra_t.pk}
    assert "id" in pk_names


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_cd_not_registered(db2_collect_tables_setup):
    conn, session, config, container, suffix, tables = db2_collect_tables_setup

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[f"{ASN_SCHEMA}.{CD_PREFIX}TD_CT_NONEXISTENT_{suffix}"],
    )
    with pytest.raises(CdcError) as exception:
        ingester.collect_tables(session)
    assert exception.value.error_code == ErrorCode.CDC4


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_invalid_spec(db2_collect_tables_setup):
    conn, session, config, container, suffix, tables = db2_collect_tables_setup

    ingester = SqlReplicationIngester(conn=conn, tables=["no_dot_here"])
    with pytest.raises(CdcError) as exc_info:
        ingester.collect_tables(session)
    assert exc_info.value.error_code == ErrorCode.CDC1


@pytest.mark.cdc_db2
@pytest.mark.slow
def test_collect_tables_type_conversion(db2_collect_tables_setup):
    conn, session, config, container, suffix, tables = db2_collect_tables_setup

    schema = config.cdc_user

    types_table = f"TD_CT_TYPES_{suffix}"
    tables.append(types_table)

    register_table_for_cdc(
        container,
        config.port,
        schema=schema,
        table=types_table,
        ddl=(
            f"create table {types_table} ("
            "col_smallint smallint, "
            "col_int integer not null primary key, "
            "col_bigint bigint, "
            "col_real real, "
            "col_double double, "
            "col_decimal decimal(10,2), "
            "col_varchar varchar(255), "
            "col_date date, "
            "col_timestamp timestamp, "
            "col_time time"
            ")"
        ),
        wait=False,
    )

    ingester = SqlReplicationIngester(
        conn=conn,
        tables=[f"{ASN_SCHEMA}.{CD_PREFIX}{types_table}"],
    )
    result = ingester.collect_tables(session)

    key = f"{schema}.{types_table.lower()}"
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

    for col in table.columns:
        assert col.iname == f"{OLD_VALUES_PREFIX}{col.name}"

    pk_names = {p.lower() for p in table.pk}
    assert "col_int" in pk_names

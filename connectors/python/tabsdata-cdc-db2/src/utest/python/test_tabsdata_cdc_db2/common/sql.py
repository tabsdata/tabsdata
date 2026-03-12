#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.cdc.common._model import TableObject
from test_tabsdata_cdc_common.common.sql import (
    CdcSampler,
    sql_literal,
)
from test_tabsdata_cdc_db2.common.fixtures.docker import register_table_for_cdc

CREATE_TABLE_DDL = (
    "create table {qualified} ("
    "pk int not null generated always as identity primary key, "
    "c_int int, "
    "c_bigint bigint, "
    "c_float real, "
    "c_double double, "
    "c_decimal decimal(10,2), "
    "c_bool smallint, "
    "c_varchar varchar(200), "
    "c_date date, "
    "c_datetime timestamp"
    ")"
)


class Db2CdcSampler(CdcSampler):

    def fold(self, name: str) -> str:
        return name.upper()

    def table_exists(self, table: TableObject) -> bool:
        self._cursor.execute(
            f"select count(*) "
            f"from syscat.tables "
            f"where tabschema = upper('{table.schema}') "
            f"and tabname = upper('{table.table}')"
        )
        return self._cursor.fetchone()[0] > 0

    def create_table(self, table: TableObject) -> None:
        register_table_for_cdc(
            container=self._container,
            port=self._port,
            schema=table.schema,
            table=table.table,
            ddl=CREATE_TABLE_DDL.format(qualified=table.qualified),
            wait=True,
        )
        # self._cursor.execute(
        #     CREATE_TABLE_DDL.format(qualified=table.qualified)
        # )

    def insert_row(self, table: TableObject, values: dict) -> int:
        i_columns = ", ".join(self._manual_columns)
        i_values = ", ".join(sql_literal(values[column]) for column in self._manual_columns)
        self._cursor.execute(
            f"insert into {table.qualified} "
            f"({i_columns}) "
            f"values ({i_values})"
        )
        self._cursor.execute("values identity_val_local()")
        return int(self._cursor.fetchone()[0])

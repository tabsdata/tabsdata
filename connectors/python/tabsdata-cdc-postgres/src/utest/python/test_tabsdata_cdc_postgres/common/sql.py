#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata.connector.cdc.common._model import TableObject
from test_tabsdata_cdc_common.common.sql import (
    CdcSampler,
    sql_literal,
)


CREATE_TABLE_DDL = (
    "create table {qualified} ("
    "pk serial primary key, "
    "c_int int, "
    "c_bigint bigint, "
    "c_float real, "
    "c_double double precision, "
    "c_decimal decimal(10,2), "
    "c_bool boolean, "
    "c_varchar varchar(200), "
    "c_date date, "
    "c_datetime timestamp"
    ")"
)


class PostgresCdcSampler(CdcSampler):

    def fold(self, name: str) -> str:
        return name

    def tx_sort_key(self, tx: str) -> int:
        return int(tx)

    def table_exists(self, table: TableObject) -> bool:
        self._cursor.execute(
            f"select count(*) "
            f"from information_schema.tables "
            f"where table_schema = '{table.schema}' "
            f"and table_name = '{table.table}'"
        )
        return self._cursor.fetchone()[0] > 0

    def create_table(self, table: TableObject) -> None:
        self._cursor.execute(
            CREATE_TABLE_DDL.format(qualified=table.qualified)
        )
        self._cursor.execute(
            f"alter table {table.qualified} replica identity full"
        )

    def insert_row(self, table: TableObject, values: dict) -> int:
        i_columns = ", ".join(self._manual_columns)
        i_values = ", ".join(sql_literal(values[column]) for column in self._manual_columns)
        self._cursor.execute(
            f"insert into {table.qualified} "
            f"({i_columns}) "
            f"values ({i_values}) "
            f"returning pk"
        )
        return self._cursor.fetchone()[0]

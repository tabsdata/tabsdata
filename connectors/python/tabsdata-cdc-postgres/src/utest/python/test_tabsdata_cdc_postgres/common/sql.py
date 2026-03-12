#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from docker.models.containers import Container

from tabsdata.connector.cdc.common._model import TableObject
from test_tabsdata_cdc_common.common.sql import (
    CdcSampler,
    DATA_COLUMNS,
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

    def table_exists(self, table: TableObject) -> bool:
        self._cursor.execute(
            f"select count(*) "
            f"from information_schema.tables "
            f"where table_schema = '{table.schema}' "
            f"and table_name = '{table.table}'"
        )
        return self._cursor.fetchone()[0] > 0

    def create_table(
        self,
        container: Container,
        port: int,
        table: TableObject
    ) -> None:
        self._cursor.execute(
            CREATE_TABLE_DDL.format(qualified=table.qualified)
        )

    def insert_row(self, table: TableObject, values: dict) -> int:
        i_columns = ", ".join(DATA_COLUMNS)
        i_values = ", ".join(sql_literal(values[column]) for column in DATA_COLUMNS)
        self._cursor.execute(
            f"insert into {table.qualified} "
            f"({i_columns}) "
            f"values ({i_values}) "
            f"returning pk"
        )
        return self._cursor.fetchone()[0]

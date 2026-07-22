#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdata.connector.cdc.common._model import TableObject
import polars as pl

from test_tabsdata_cdc_common.common.evolution import (
    ADD_COLUMN_TARGETS,
    CHANGE_TYPE_TARGETS,
    DROP_COLUMN_TARGETS,
    RENAME_COLUMN_TARGETS,
    SchemaChange,
    SchemaChangeKind,
)
from test_tabsdata_cdc_common.common.sql import (
    CdcSampler,
    sql_literal,
)


CREATE_TABLE_DDL = (
    "create table {qualified} ("
    "pk int auto_increment primary key, "
    "c_int int, "
    "c_bigint bigint, "
    "c_float float, "
    "c_double double, "
    "c_decimal decimal(10,2), "
    "c_bool boolean, "
    "c_varchar varchar(200), "
    "c_date date, "
    "c_datetime datetime"
    ")"
)


class MySQLCdcSampler(CdcSampler):

    def fold(self, name: str) -> str:
        return name

    def tx_sort_key(self, tx: str) -> tuple[str, int]:
        uuid, seq = tx.rsplit(":", 1)
        return uuid, int(seq)

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

    def insert_row(self, table: TableObject, values: dict) -> int:
        i_columns = ", ".join(self._manual_columns)
        i_values = ", ".join(sql_literal(values[column]) for column in self._manual_columns)
        self._cursor.execute(
            f"insert into {table.qualified} "
            f"({i_columns}) "
            f"values ({i_values})"
        )
        return self._cursor.lastrowid

    def build_schema_changes(
        self,
        table: TableObject,
        kinds: list[SchemaChangeKind],
        iteration: int,
    ) -> list[SchemaChange]:
        changes = []
        qtable = table.qualified
        for kind in kinds:
            if kind == SchemaChangeKind.ADD_COLUMN:
                column_name, column_type = ADD_COLUMN_TARGETS[iteration]
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=f"alter table {qtable} add column {column_name} {column_type}",
                    new_column=column_name,
                ))
            elif kind == SchemaChangeKind.DROP_COLUMN:
                column_name = DROP_COLUMN_TARGETS[iteration]
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=f"alter table {qtable} drop column {column_name}",
                    old_column=column_name,
                ))
            elif kind == SchemaChangeKind.RENAME_COLUMN:
                old_name, new_name = RENAME_COLUMN_TARGETS[iteration]
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=(
                        f"alter table {qtable} rename column {old_name} to {new_name}"
                    ),
                    old_column=old_name,
                    new_column=new_name,
                ))
            elif kind == SchemaChangeKind.CHANGE_TYPE:
                column_name, new_pl_type = CHANGE_TYPE_TARGETS[iteration]
                type_sql = mysql_type_sql(new_pl_type)
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=(
                        f"alter table {qtable} modify column {column_name} {type_sql}"
                    ),
                    old_column=column_name,
                    new_type_name=type_sql,
                    new_polars_type=new_pl_type,
                ))
        return changes


def mysql_type_sql(pl_type) -> str:
    if pl_type == pl.Int64:
        return "bigint"
    if pl_type == pl.Float64:
        return "double"
    if isinstance(pl_type, pl.Decimal):
        return f"decimal({pl_type.precision},{pl_type.scale})"
    raise ValueError(f"Unsupported MySQL type mapping for {pl_type}")

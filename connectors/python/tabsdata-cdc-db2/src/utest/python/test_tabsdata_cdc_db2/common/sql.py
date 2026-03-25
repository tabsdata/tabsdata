#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from typing import Any

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
from test_tabsdata_cdc_db2.common.fixtures.docker import (
    CdcMode,
    deregister_table_from_cdc,
    register_table_for_cdc,
    reregister_table_for_cdc,
)

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

    def __init__(self, container, port, cursor):
        super().__init__(container, port, cursor)
        self._cd_table_version = 0
        self._cd_qualified_table: str | None = None

    def fold(self, name: str) -> str:
        return name.upper()

    def tx_sort_key(self, tx: str) -> bytes:
        return bytes.fromhex(tx)

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
                    alter_sql=f"alter table {qtable} add column {column_name.upper()} {column_type.upper()}",
                    new_column=column_name,
                ))
            elif kind == SchemaChangeKind.DROP_COLUMN:
                column_name = DROP_COLUMN_TARGETS[iteration]
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=f"alter table {qtable} drop column {column_name.upper()}",
                    old_column=column_name,
                ))
            elif kind == SchemaChangeKind.RENAME_COLUMN:
                old_name, new_name = RENAME_COLUMN_TARGETS[iteration]
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=f"alter table {qtable} rename column {old_name.upper()} to {new_name.upper()}",
                    old_column=old_name,
                    new_column=new_name,
                ))
            elif kind == SchemaChangeKind.CHANGE_TYPE:
                column_name, new_pl_type = CHANGE_TYPE_TARGETS[iteration]
                type_sql = db2_type_sql(new_pl_type)
                changes.append(SchemaChange(
                    kind=kind,
                    alter_sql=f"alter table {qtable} alter column {column_name.upper()} set data type {type_sql}",
                    old_column=column_name,
                    new_type_name=type_sql,
                    new_polars_type=new_pl_type,
                ))
        return changes

    def alter_table(
        self,
        connection: Any,
        table: TableObject,
        changes: list[SchemaChange],
    ) -> str:
        self._cd_table_version += 1
        deregister_table_from_cdc(
            container=self._container,
            port=self._port,
            schema=table.schema,
            table=table.table,
            mode=CdcMode.HARD,
        )
        for change in changes:
            self._cursor.execute(change.alter_sql)
        connection.commit()
        self._cursor.execute(
            f"call sysproc.admin_cmd('reorg table {table.qualified}')"
        )
        connection.commit()
        new_cd_table = reregister_table_for_cdc(
            container=self._container,
            port=self._port,
            schema=table.schema,
            table=table.table,
            wait=True,
            mode=CdcMode.HARD,
            version=self._cd_table_version,
        )
        self.apply_schema_changes(changes)
        self._cd_qualified_table = new_cd_table
        return new_cd_table


def db2_type_sql(pl_type) -> str:
    if pl_type == pl.Int64:
        return "bigint"
    if pl_type == pl.Float64:
        return "double"
    if isinstance(pl_type, pl.Decimal):
        return f"decimal({pl_type.precision},{pl_type.scale})"
    raise ValueError(f"Unsupported DB2 type mapping for {pl_type}")

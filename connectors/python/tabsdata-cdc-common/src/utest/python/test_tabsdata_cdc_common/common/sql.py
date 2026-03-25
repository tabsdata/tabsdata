#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import random
import string
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from test_tabsdata_cdc_common.common.evolution import (
        SchemaChange,
        SchemaChangeKind,
    )

logger = logging.getLogger(__name__)

import polars as pl
from docker.models.containers import Container

from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import CdcOperation

AUTO_COLUMNS = [
    "pk",
]

MANUAL_COLUMNS = [
    "c_int",
    "c_bigint",
    "c_float",
    "c_double",
    "c_decimal",
    "c_bool",
    "c_varchar",
    "c_date",
    "c_datetime",
]

COLUMNS = AUTO_COLUMNS + MANUAL_COLUMNS

COLUMN_TYPES: dict[str, Any] = {
    "pk": pl.Int32,
    "c_int": pl.Int32,
    "c_bigint": pl.Int64,
    "c_float": pl.Float32,
    "c_double": pl.Float64,
    "c_decimal": pl.Decimal(10, 2),
    "c_bool": pl.Boolean,
    "c_varchar": pl.Utf8,
    "c_date": pl.Date,
    "c_datetime": pl.Datetime,
}

FLOAT_COLUMNS = {
    name for name, dtype in COLUMN_TYPES.items()
    if dtype in (pl.Float16, pl.Float32, pl.Float64)
}

EPOCH = date(2020, 1, 1)

VALUE_GENERATORS: dict[Any, Callable] = {
    pl.Int32: lambda seq: seq * 10 + random.randint(1, 10_000),
    pl.Int64: lambda seq: seq * 100_000 + random.randint(1, 10_000),
    pl.Float32: lambda seq: float(seq) + round(random.random(), 4),
    pl.Float64: lambda seq: float(seq) + round(random.random(), 8),
    pl.Decimal(10, 2): lambda seq: round(seq * 1.11 + random.random(), 2),
    pl.Boolean: lambda seq: random.choice([True, False]),
    pl.Utf8: lambda seq: f"row_{seq}_{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}",
    pl.Date: lambda seq: EPOCH + timedelta(days=random.randint(0, 2000)),
    pl.Datetime: lambda seq: datetime.combine(
        EPOCH + timedelta(days=random.randint(0, 2000)),
        datetime.min.time(),
    ).replace(hour=random.randint(0, 23), minute=random.randint(0, 59)),
}


def generate_value(dtype: Any, seq: int) -> Any:
    dtype_map = {
        pl.Int32: VALUE_GENERATORS[pl.Int32],
        pl.Int64: VALUE_GENERATORS[pl.Int64],
        pl.Float32: VALUE_GENERATORS[pl.Float32],
        pl.Float64: VALUE_GENERATORS[pl.Float64],
        pl.Boolean: VALUE_GENERATORS[pl.Boolean],
        pl.Utf8: VALUE_GENERATORS[pl.Utf8],
        pl.Date: VALUE_GENERATORS[pl.Date],
        pl.Datetime: VALUE_GENERATORS[pl.Datetime],
    }
    if dtype in dtype_map:
        return dtype_map[dtype](seq)
    if isinstance(dtype, pl.Decimal) or dtype == pl.Decimal:
        return round(seq * 1.11 + random.random(), 2)
    if isinstance(dtype, pl.Datetime) or dtype == pl.Datetime:
        return VALUE_GENERATORS[pl.Datetime](seq)
    raise ValueError(f"No value generator for dtype {dtype}")


def random_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def random_date() -> date:
    return EPOCH + timedelta(days=random.randint(0, 2000))


def random_datetime() -> datetime:
    return datetime.combine(
        random_date(),
        datetime.min.time(),
    ).replace(hour=random.randint(0, 23), minute=random.randint(0, 59))


def values_for_insert(fold: Callable, seq: int) -> dict:
    rand = random.randint(1, 10_000)
    return {
        fold("c_int"): seq * 10 + rand,
        fold("c_bigint"): seq * 100_000 + rand,
        fold("c_float"): float(seq) + round(random.random(), 4),
        fold("c_double"): float(seq) + round(random.random(), 8),
        fold("c_decimal"): round(seq * 1.11 + random.random(), 2),
        fold("c_bool"): random.choice([True, False]),
        fold("c_varchar"): f"row_{seq}_{random_suffix()}",
        fold("c_date"): random_date(),
        fold("c_datetime"): random_datetime(),
    }


def values_for_update(fold: Callable, seq: int) -> dict:
    rand = random.randint(1, 10_000)
    return {
        fold("c_int"): seq * 20 + rand,
        fold("c_bigint"): seq * 200_000 + rand,
        fold("c_float"): float(seq) + round(random.random(), 4),
        fold("c_double"): float(seq) + round(random.random(), 8),
        fold("c_decimal"): round(seq * 2.22 + random.random(), 2),
        fold("c_bool"): random.choice([True, False]),
        fold("c_varchar"): f"updated_{seq}_{random_suffix()}",
        fold("c_date"): random_date(),
        fold("c_datetime"): random_datetime(),
    }


def sql_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    return f"'{value}'"


def null_row(columns: list[str]) -> dict:
    return {column: None for column in columns}


def change_record(
    op: str,
    new_values: dict,
    old_values: dict,
) -> dict:
    return {
        "op": op,
        **{f"new.{column}": value for column, value in new_values.items()},
        **{f"old.{column}": value for column, value in old_values.items()},
    }


class CdcSampler(ABC):

    def __init__(self, container: Container, port: int, cursor: Any):
        self._container = container
        self._port = port
        self._cursor = cursor
        self._data: dict[TableObject, pl.DataFrame] = {}
        self._changes: dict[TableObject, pl.DataFrame] = {}
        self._inserted_pks: dict[TableObject, set[int]] = {}
        self._columns = [self.fold(column) for column in COLUMNS]
        self._auto_columns = [
            self.fold(column) for column in AUTO_COLUMNS
        ]
        self._manual_columns = [
            self.fold(column) for column in MANUAL_COLUMNS
        ]
        self._column_types = {
            self.fold(column): ctype
            for column, ctype in COLUMN_TYPES.items()
        }
        self._float_columns = [
            self.fold(column) for column in FLOAT_COLUMNS
        ]

    @abstractmethod
    def fold(self, name: str) -> str: ...

    @abstractmethod
    def tx_sort_key(self, tx: str) -> Any: ...

    @property
    def columns(self) -> list[str]:
        return self._columns

    @property
    def auto_columns(self) -> list[str]:
        return self._auto_columns

    @property
    def manual_columns(self) -> list[str]:
        return self._manual_columns

    @property
    def float_columns(self) -> list[str]:
        return self._float_columns

    @property
    def column_types(self) -> dict[str, pl.DataType]:
        return self._column_types

    @property
    def container(self) -> Container:
        return self._container

    @property
    def port(self) -> int:
        return self._port

    @property
    def cursor(self) -> Any:
        return self._cursor

    @property
    def data(self) -> dict[TableObject, pl.DataFrame]:
        return self._data

    @property
    def changes(self) -> dict[TableObject, pl.DataFrame]:
        return self._changes

    def t_data(self, table: TableObject) -> pl.DataFrame:
        return self._data.get(table, pl.DataFrame())

    def t_changes(self, table: TableObject) -> pl.DataFrame:
        return self._changes.get(table, pl.DataFrame())

    @abstractmethod
    def table_exists(self, table: TableObject) -> bool: ...

    @abstractmethod
    def create_table(self, table: TableObject) -> None: ...

    @abstractmethod
    def insert_row(self, table: TableObject, values: dict) -> int: ...

    def fetch_all_rows(self, table: TableObject) -> list[dict]:
        columns = ", ".join(self._columns)
        self._cursor.execute(
            f"select {columns} "
            f"from {table.qualified}")
        rows = self._cursor.fetchall()
        return [
            {column: value for column, value in zip(self._columns, row)}
            for row in rows
        ]

    def update_row(self, table: TableObject, pk: int, values: dict) -> int:
        clause = ", ".join(
            f"{column} = {sql_literal(values[column])}"
            for column in self._manual_columns
        )
        pk_column = self.fold("pk")
        self._cursor.execute(
            f"update {table.qualified} "
            f"set {clause} "
            f"where {pk_column} = {pk}"
        )
        return self._cursor.rowcount

    def delete_row(self, table: TableObject, pk: int) -> int:
        pk_column = self.fold("pk")
        self._cursor.execute(
            f"delete from {table.qualified} "
            f"where {pk_column} = {pk}"
        )
        return self._cursor.rowcount

    def run(
        self,
        table: TableObject,
        inserts: int,
        updates: int,
        deletes: int,
    ) -> None:
        if not self.table_exists(table):
            self.create_table(table)

        pk_column = self.fold("pk")
        changes: list[dict] = []

        own_pks = self._inserted_pks.setdefault(table, set())

        for seq in range(1, inserts + 1):
            insert_values = values_for_insert(self.fold, seq)
            pk = self.insert_row(table, insert_values)
            own_pks.add(pk)
            all_values = {pk_column: pk, **insert_values}
            changes.append(
                change_record(
                    CdcOperation.INSERT, all_values, null_row(self._columns),
                )
            )

        if updates > 0:
            all_rows = self.fetch_all_rows(table)
            all_rows = [
                row for row in all_rows
                if row[pk_column] in own_pks
            ]
            if updates > len(all_rows):
                raise ValueError(
                    f"updates ({updates}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            update_rows = random.sample(all_rows, updates)
            for seq, old_values in enumerate(update_rows, start=1):
                pk = old_values[pk_column]
                update_values = values_for_update(self.fold, seq)
                all_values = {pk_column: pk, **update_values}
                affected = self.update_row(table, pk, update_values)
                if affected > 0:
                    changes.append(
                        change_record(CdcOperation.UPDATE, all_values, old_values)
                    )
                else:
                    logger.debug(
                        f"[NOOP] UPDATE {table.qualified} pk={pk} "
                        f"affected 0 rows"
                    )

        if deletes > 0:
            all_rows = self.fetch_all_rows(table)
            all_rows = [
                row for row in all_rows
                if row[pk_column] in own_pks
            ]
            if deletes > len(all_rows):
                raise ValueError(
                    f"deletes ({deletes}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            delete_rows = random.sample(all_rows, deletes)
            for old_values in delete_rows:
                pk = old_values[pk_column]
                affected = self.delete_row(table, pk)
                if affected > 0:
                    own_pks.discard(pk)
                    changes.append(
                        change_record(
                            CdcOperation.DELETE, old_values, null_row(self._columns),
                        )
                    )
                else:
                    logger.debug(
                        f"[NOOP] DELETE {table.qualified} pk={pk} "
                        f"affected 0 rows"
                    )

        remaining = self.fetch_all_rows(table)
        self._data[table] = pl.DataFrame(
            remaining, schema=self._column_types,
        )

        changes_df = pl.DataFrame(
            changes,
            schema={
                "op": pl.Utf8,
                **{f"new.{column}": ctype for column, ctype in self._column_types.items()},
                **{f"old.{column}": ctype for column, ctype in self._column_types.items()},
            },
        )
        existing = self._changes.get(table)
        if existing is not None and existing.height > 0:
            self._changes[table] = pl.concat(
                [existing, changes_df], how="diagonal"
            )
        else:
            self._changes[table] = changes_df

    def build_schema_changes(
        self,
        table: TableObject,
        kinds: list[SchemaChangeKind],
        iteration: int,
    ) -> list[SchemaChange]:
        raise NotImplementedError(
            "Subclasses must implement build_schema_changes"
        )

    def apply_schema_changes(self, changes: list[SchemaChange]) -> None:
        from test_tabsdata_cdc_common.common.evolution import SchemaChangeKind

        for change in changes:
            col_fold = self.fold
            if change.kind == SchemaChangeKind.ADD_COLUMN:
                new_col = col_fold(change.new_column)
                self._manual_columns.append(new_col)
                self._columns.append(new_col)
                self._column_types[new_col] = pl.Utf8
            elif change.kind == SchemaChangeKind.DROP_COLUMN:
                old_col = col_fold(change.old_column)
                if old_col in self._manual_columns:
                    self._manual_columns.remove(old_col)
                if old_col in self._columns:
                    self._columns.remove(old_col)
                self._column_types.pop(old_col, None)
                if old_col in self._float_columns:
                    self._float_columns.remove(old_col)
            elif change.kind == SchemaChangeKind.RENAME_COLUMN:
                old_col = col_fold(change.old_column)
                new_col = col_fold(change.new_column)
                if old_col in self._manual_columns:
                    idx = self._manual_columns.index(old_col)
                    self._manual_columns[idx] = new_col
                if old_col in self._columns:
                    idx = self._columns.index(old_col)
                    self._columns[idx] = new_col
                if old_col in self._column_types:
                    self._column_types[new_col] = self._column_types.pop(old_col)
                if old_col in self._float_columns:
                    idx = self._float_columns.index(old_col)
                    self._float_columns[idx] = new_col
            elif change.kind == SchemaChangeKind.CHANGE_TYPE:
                col = col_fold(change.old_column or change.new_column)
                if change.new_polars_type is not None:
                    self._column_types[col] = change.new_polars_type
                    if change.new_polars_type in (pl.Float32, pl.Float64):
                        if col not in self._float_columns:
                            self._float_columns.append(col)
                    else:
                        if col in self._float_columns:
                            self._float_columns.remove(col)

    def alter_table(
        self,
        connection: Any,
        table: TableObject,
        changes: list[SchemaChange],
    ) -> None:
        for change in changes:
            self._cursor.execute(change.alter_sql)
        connection.commit()
        self.apply_schema_changes(changes)

    def dynamic_values_for_insert(self, seq: int) -> dict:
        values = {}
        for col in self._manual_columns:
            dtype = self._column_types.get(col, pl.Utf8)
            values[col] = generate_value(dtype, seq)
        return values

    def dynamic_values_for_update(self, seq: int) -> dict:
        values = {}
        for col in self._manual_columns:
            dtype = self._column_types.get(col, pl.Utf8)
            values[col] = generate_value(dtype, seq + 1000)
        return values

    def run_dynamic(
        self,
        table: TableObject,
        inserts: int,
        updates: int = 0,
        deletes: int = 0,
    ) -> None:
        if not self.table_exists(table):
            self.create_table(table)

        pk_column = self.fold("pk")
        changes: list[dict] = []

        own_pks = self._inserted_pks.setdefault(table, set())

        for seq in range(1, inserts + 1):
            insert_values = self.dynamic_values_for_insert(seq)
            pk = self.insert_row(table, insert_values)
            own_pks.add(pk)
            all_values = {pk_column: pk, **insert_values}
            changes.append(
                change_record(
                    CdcOperation.INSERT, all_values, null_row(self._columns),
                )
            )

        if updates > 0:
            all_rows = self.fetch_all_rows(table)
            all_rows = [
                row for row in all_rows
                if row[pk_column] in own_pks
            ]
            if updates > len(all_rows):
                raise ValueError(
                    f"updates ({updates}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            update_rows = random.sample(all_rows, updates)
            for seq, old_values in enumerate(update_rows, start=1):
                pk = old_values[pk_column]
                update_values = self.dynamic_values_for_update(seq)
                all_values = {pk_column: pk, **update_values}
                affected = self.update_row(table, pk, update_values)
                if affected > 0:
                    changes.append(
                        change_record(
                            CdcOperation.UPDATE, all_values, old_values,
                        )
                    )
                else:
                    logger.debug(
                        f"[NOOP] UPDATE {table.qualified} pk={pk} "
                        f"affected 0 rows"
                    )

        if deletes > 0:
            all_rows = self.fetch_all_rows(table)
            all_rows = [
                row for row in all_rows
                if row[pk_column] in own_pks
            ]
            if deletes > len(all_rows):
                raise ValueError(
                    f"deletes ({deletes}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            delete_rows = random.sample(all_rows, deletes)
            for old_values in delete_rows:
                pk = old_values[pk_column]
                affected = self.delete_row(table, pk)
                if affected > 0:
                    own_pks.discard(pk)
                    changes.append(
                        change_record(
                            CdcOperation.DELETE, old_values,
                            null_row(self._columns),
                        )
                    )
                else:
                    logger.debug(
                        f"[NOOP] DELETE {table.qualified} pk={pk} "
                        f"affected 0 rows"
                    )

        remaining = self.fetch_all_rows(table)
        self._data[table] = pl.DataFrame(
            remaining, schema=self._column_types,
        )

        changes_df = pl.DataFrame(
            changes,
            schema={
                "op": pl.Utf8,
                **{f"new.{column}": ctype for column, ctype in self._column_types.items()},
                **{f"old.{column}": ctype for column, ctype in self._column_types.items()},
            },
        )
        existing = self._changes.get(table)
        if existing is not None and existing.height > 0:
            self._changes[table] = pl.concat(
                [existing, changes_df], how="diagonal"
            )
        else:
            self._changes[table] = changes_df

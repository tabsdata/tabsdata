#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import random
import string
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Any

import polars as pl
from docker.models.containers import Container

from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import CdcOperation

COLUMNS = [
    "pk",
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

DATA_COLUMNS = [c for c in COLUMNS if c != "pk"]

COLUMN_TYPES: dict[str, pl.DataType] = {
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

EPOCH = date(2020, 1, 1)


def random_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


def random_date() -> date:
    return EPOCH + timedelta(days=random.randint(0, 2000))


def random_datetime() -> datetime:
    return datetime.combine(
        random_date(),
        datetime.min.time(),
    ).replace(hour=random.randint(0, 23), minute=random.randint(0, 59))


def values_for_insert(seq: int) -> dict:
    rand = random.randint(1, 10_000)
    return {
        "c_int": seq * 10 + rand,
        "c_bigint": seq * 100_000 + rand,
        "c_float": float(seq) + round(random.random(), 4),
        "c_double": float(seq) + round(random.random(), 8),
        "c_decimal": round(seq * 1.11 + random.random(), 2),
        "c_bool": random.choice([True, False]),
        "c_varchar": f"row_{seq}_{random_suffix()}",
        "c_date": random_date(),
        "c_datetime": random_datetime(),
    }


def values_for_update(seq: int) -> dict:
    rand = random.randint(1, 10_000)
    return {
        "c_int": seq * 20 + rand,
        "c_bigint": seq * 200_000 + rand,
        "c_float": float(seq) + round(random.random(), 4),
        "c_double": float(seq) + round(random.random(), 8),
        "c_decimal": round(seq * 2.22 + random.random(), 2),
        "c_bool": random.choice([True, False]),
        "c_varchar": f"updated_{seq}_{random_suffix()}",
        "c_date": random_date(),
        "c_datetime": random_datetime(),
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


def null_row() -> dict:
    return {column: None for column in COLUMNS}


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

    def __init__(self, cursor: Any):
        self._cursor = cursor
        self._data: dict[TableObject, pl.DataFrame] = {}
        self._changes: dict[TableObject, pl.DataFrame] = {}

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
    def create_table(
        self,
        container: Container,
        port: int,
        table: TableObject
    ) -> None: ...

    @abstractmethod
    def insert_row(self, table: TableObject, values: dict) -> int: ...

    def fetch_all_rows(self, table: TableObject) -> list[dict]:
        columns = ", ".join(COLUMNS)
        self._cursor.execute(
            f"select {columns} "
            f"from {table.qualified}")
        rows = self._cursor.fetchall()
        return [{column: value for column, value in zip(COLUMNS, row)} for row in rows]

    def update_row(self, table: TableObject, pk: int, values: dict) -> None:
        clause = ", ".join(
            f"{column} = {sql_literal(values[column])}" for column in DATA_COLUMNS
        )
        self._cursor.execute(
            f"update {table.qualified} "
            f"set {clause} "
            f"where pk = {pk}"
        )

    def delete_row(self, table: TableObject, pk: int) -> None:
        self._cursor.execute(
            f"delete from {table.qualified} "
            f"where pk = {pk}"
        )

    def run(
        self,
        container: Container,
        port: int,
        table: TableObject,
        inserts: int,
        updates: int,
        deletes: int,
    ) -> None:
        if not self.table_exists(table):
            self.create_table(
                container=container,
                port=port,
                table=table)

        changes: list[dict] = []

        for seq in range(1, inserts + 1):
            insert_values = values_for_insert(seq)
            pk = self.insert_row(table, insert_values)
            all_values = {"pk": pk, **insert_values}
            changes.append(
                change_record(CdcOperation.INSERT, all_values, null_row())
            )

        if updates > 0:
            all_rows = self.fetch_all_rows(table)
            if updates > len(all_rows):
                raise ValueError(
                    f"updates ({updates}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            update_rows = random.sample(all_rows, updates)
            for seq, old_values in enumerate(update_rows, start=1):
                pk = old_values["pk"]
                update_values = values_for_update(seq)
                all_values = {"pk": pk, **update_values}
                self.update_row(table, pk, update_values)
                changes.append(
                    change_record(CdcOperation.UPDATE, all_values, old_values)
                )

        if deletes > 0:
            all_rows = self.fetch_all_rows(table)
            if deletes > len(all_rows):
                raise ValueError(
                    f"deletes ({deletes}) exceeds existing row count "
                    f"({len(all_rows)})"
                )
            delete_rows = random.sample(all_rows, deletes)
            for old_values in delete_rows:
                pk = old_values["pk"]
                self.delete_row(table, pk)
                changes.append(
                    change_record(CdcOperation.DELETE, old_values, null_row())
                )

        remaining = self.fetch_all_rows(table)
        self._data[table] = pl.DataFrame(
            remaining, schema=COLUMN_TYPES,
        )

        changes_df = pl.DataFrame(
            changes,
            schema={
                "op": pl.Utf8,
                **{f"new.{c_column}": c_type for c_column, c_type in COLUMN_TYPES.items()},
                **{f"old.{c_column}": c_type for c_column, c_type in COLUMN_TYPES.items()},
            },
        )
        existing = self._changes.get(table)
        if existing is not None and existing.height > 0:
            self._changes[table] = pl.concat(
                [existing, changes_df], how="diagonal"
            )
        else:
            self._changes[table] = changes_df

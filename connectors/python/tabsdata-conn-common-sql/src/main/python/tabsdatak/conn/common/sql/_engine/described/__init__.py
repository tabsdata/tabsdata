#
# Copyright 2026 Tabsdata Inc.
#

"""Column dtypes for a SQL source, read off the cursor description its driver
returns with a query's result, before any row is built.

Each column is typed by the first of these that pins it down:

1. The describer for the driver (SQLAlchemy's ``dialect.driver``), which names
   the column's SQL type from the driver's own type code. The built-in ones are
   in :mod:`.drivers`; a driver they do not cover gets one from the
   :data:`ENTRY_POINT_GROUP` entry points or :func:`register`, so it is
   supported without a new release. A built-in driver whose description leaves
   something out has it completed first, from its cursor or its database.
2. What the driver reports itself, when its type code is a SQL type name or the
   Python class of its values.
3. Neither: the column is left out, to be inferred over every row of its batch.

Every SQL type name, whoever produced it, is mapped by :mod:`.commons.names`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from enum import StrEnum
from functools import cache
from importlib.metadata import entry_points
from typing import Any, NamedTuple

import polars as pl

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.described.commons import names

logger = logging.getLogger(__name__)


class Column(NamedTuple):
    """One entry of a cursor description: the seven fields DB-API 2.0 (PEP 249)
    defines, then whatever the driver adds after them, in ``extra``. ``entry``
    is the entry itself, for a driver whose entries carry more than a tuple."""

    name: str
    code: Any
    display_size: Any = None
    internal_size: Any = None
    precision: Any = None
    scale: Any = None
    null_ok: Any = None
    extra: tuple[Any, ...] = ()
    entry: Any = None

    @classmethod
    def of(cls, entry: Sequence[Any]) -> Column:
        standard = tuple(entry[:7])
        return cls(*standard, extra=tuple(entry[7:]), entry=entry)


class Query(NamedTuple):
    """The query a description belongs to, for a driver whose description is
    completed from its cursor or its database."""

    connection: Any
    cursor: Any
    text: str
    parameters: Mapping[str, Any]


Describer = Callable[[Column], "str | pl.DataType | None"]
"""Names the SQL type of one described column, or ``None`` when its
description does not tell. The name may carry its parameters, as in
``DECIMAL(10, 2)`` or ``BIGINT UNSIGNED``; a type no SQL name stands for, such
as a list, is given as its dtype."""

Completer = Callable[[Query, list[Column]], list[Column]]
"""Fills in what a driver's description leaves out."""

ENTRY_POINT_GROUP = "tabsdatak.sql.describers"
"""Entry points registering a :data:`Describer` under the driver name it
describes."""


class Driver(StrEnum):
    """The drivers with a built-in describer, by SQLAlchemy's driver name."""

    MYSQLCONNECTOR = "mysqlconnector"
    MARIADBCONNECTOR = "mariadbconnector"
    PYMYSQL = "pymysql"
    MYSQLDB = "mysqldb"
    PSYCOPG2 = "psycopg2"
    PSYCOPG2CFFI = "psycopg2cffi"
    PSYCOPG = "psycopg"
    PG8000 = "pg8000"
    PYODBC = "pyodbc"
    PYMSSQL = "pymssql"
    ORACLEDB = "oracledb"
    CX_ORACLE = "cx_oracle"
    IBM_DB = "ibm_db_sa"


@cache
def _built_in() -> dict[str, Describer]:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described.drivers import (
        db2,
        mssql,
        mysql,
        odbc,
        oracle,
        postgres,
    )

    return {
        Driver.MYSQLCONNECTOR: mysql.describe,
        Driver.MARIADBCONNECTOR: mysql.describe,
        Driver.PYMYSQL: mysql.describe,
        Driver.MYSQLDB: mysql.describe,
        Driver.PSYCOPG2: postgres.describe,
        Driver.PSYCOPG2CFFI: postgres.describe,
        Driver.PSYCOPG: postgres.describe,
        Driver.PG8000: postgres.describe,
        Driver.PYODBC: odbc.describe,
        Driver.PYMSSQL: mssql.pymssql,
        Driver.ORACLEDB: oracle.describe,
        Driver.CX_ORACLE: oracle.describe,
        Driver.IBM_DB: db2.describe,
    }


@cache
def _completers() -> dict[str, Completer]:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine.described.drivers import mysql

    return {
        Driver.PYMYSQL: mysql.from_fields,
        Driver.MYSQLCONNECTOR: mysql.from_database,
    }


_registered: dict[str, Describer] = {}


def _from_entry_point(driver: str) -> Describer | None:
    for point in entry_points(group=ENTRY_POINT_GROUP, name=driver):
        try:
            return point.load()
        except (ImportError, AttributeError):
            logger.warning(
                "Cannot load the describer %r for SQL driver %r",
                point.value,
                driver,
                exc_info=True,
            )
    return None


def register(driver: str, describe: Describer) -> None:
    """Describe ``driver``'s columns with ``describe``. A driver that already
    has a describer -- built in, registered or from an entry point -- keeps it,
    with a warning."""
    driver = str(driver)
    if driver in _built_in() or driver in _registered or _from_entry_point(driver):
        logger.warning(
            "SQL driver %r already has a describer; ignoring the new one", driver
        )
        return
    _registered[driver] = describe


def describer(driver: str) -> Describer | None:
    """The describer for ``driver``: built in, then registered, then from an
    entry point. An entry point naming a driver with a built-in describer is
    ignored, with a warning."""
    driver = str(driver)
    if driver in _built_in():
        if any(entry_points(group=ENTRY_POINT_GROUP, name=driver)):
            logger.warning(
                "SQL driver %r has a built-in describer; ignoring its entry point",
                driver,
            )
        return _built_in()[driver]
    if driver in _registered:
        return _registered[driver]
    return _from_entry_point(driver)


def columns(
    driver: str, description: Sequence[Sequence[Any]] | None, query: Query | None
) -> list[Column]:
    """``description``'s columns, completed from ``query`` when ``driver``'s
    description leaves something out. A completion that fails leaves the
    description as it is."""
    described = [Column.of(entry) for entry in description or ()]
    complete = _completers().get(str(driver))
    if complete is None or query is None or not described:
        return described
    try:
        completed = complete(query, described)
    except Exception:  # noqa: BLE001 -- the description alone still types the read
        logger.debug(
            "Cannot complete the description of SQL driver %r", driver, exc_info=True
        )
        return described
    return completed if len(completed) == len(described) else described


def dtypes(
    driver: str,
    description: Sequence[Sequence[Any]] | None,
    query: Query | None = None,
) -> dict[str, pl.DataType]:
    """The dtype of each column ``description`` pins down, by column name. A
    column it does not is left out, with a warning: the reader types it from
    its values, over every row of each batch."""
    driver = str(driver)
    described = columns(driver, description, query)
    if not described:
        return {}
    describe = describer(driver)
    typed: dict[str, pl.DataType] = {}
    untyped: list[str] = []
    for column in described:
        dtype = _dtype(describe, column)
        if dtype is None:
            untyped.append(column.name)
        else:
            typed[column.name] = dtype
    if untyped:
        logger.warning(
            "The description of SQL driver %r gives no type for column(s) %s;"
            " they are typed from their values in each batch",
            driver,
            ", ".join(repr(name) for name in untyped),
        )
    logger.debug(
        "Typed %d of %d column(s) from the description of SQL driver %r (%s)",
        len(typed),
        len(described),
        driver,
        "with a describer" if describe is not None else "without a describer",
    )
    return typed


def _dtype(describe: Describer | None, column: Column) -> pl.DataType | None:
    if describe is not None:
        described = describe(column)
        if isinstance(described, pl.DataType):
            return described
        if described is not None:
            return _named(column, described)
    if isinstance(column.code, str):
        return _named(column, column.code, column.precision, column.scale)
    if isinstance(column.code, type):
        return names.from_class(column.code, column.precision, column.scale)
    return None


def _named(
    column: Column, name: str, precision: Any = None, scale: Any = None
) -> pl.DataType | None:
    dtype = names.dtype(name, precision, scale)
    if names.too_wide(name, precision):
        logger.warning(
            "Column %r is a %s, wider than the %d digits a decimal holds;"
            " it is read as text",
            column.name,
            name,
            names.MAX_DECIMAL_PRECISION,
        )
    return dtype

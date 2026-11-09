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
   supported without a new release.
2. What the driver reports itself, when its type code is a SQL type name or the
   Python class of its values.
3. Neither: the column is left out, to be inferred over every row of its batch.

Every SQL type name, whoever produced it, is mapped by :mod:`.commons.names`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
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
    defines, then whatever the driver adds after them, in ``extra``."""

    name: str
    code: Any
    display_size: Any = None
    internal_size: Any = None
    precision: Any = None
    scale: Any = None
    null_ok: Any = None
    extra: tuple[Any, ...] = ()

    @classmethod
    def of(cls, entry: Sequence[Any]) -> Column:
        standard = tuple(entry[:7])
        return cls(*standard, extra=tuple(entry[7:]))


Describer = Callable[[Column], str | None]
"""Names the SQL type of one described column, or ``None`` when its
description does not tell. The name may carry its parameters, as in
``DECIMAL(10, 2)`` or ``BIGINT UNSIGNED``."""

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
        Driver.MYSQLCONNECTOR: mysql.mysqlconnector,
        Driver.MARIADBCONNECTOR: mysql.mariadbconnector,
        Driver.PYMYSQL: mysql.flagless,
        Driver.MYSQLDB: mysql.flagless,
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


def dtypes(
    driver: str, description: Sequence[Sequence[Any]] | None
) -> dict[str, pl.DataType]:
    """The dtype of each column ``description`` pins down, by column name. A
    column it does not is left out, with a warning: the reader types it from
    its values, over every row of each batch."""
    if not description:
        return {}
    driver = str(driver)
    describe = describer(driver)
    described: dict[str, pl.DataType] = {}
    untyped: list[str] = []
    for entry in description:
        column = Column.of(entry)
        dtype = _dtype(describe, column)
        if dtype is None:
            untyped.append(column.name)
        else:
            described[column.name] = dtype
    if untyped:
        logger.warning(
            "The description of SQL driver %r gives no type for column(s) %s;"
            " they are typed from their values in each batch",
            driver,
            ", ".join(repr(name) for name in untyped),
        )
    logger.debug(
        "Typed %d of %d column(s) from the description of SQL driver %r (%s)",
        len(described),
        len(description),
        driver,
        "with a describer" if describe is not None else "without a describer",
    )
    return described


def _dtype(describe: Describer | None, column: Column) -> pl.DataType | None:
    if describe is not None:
        name = describe(column)
        if name is not None:
            return names.dtype(name)
    if isinstance(column.code, str):
        return names.dtype(column.code, column.precision, column.scale)
    if isinstance(column.code, type):
        return names.from_class(column.code, column.precision, column.scale)
    return None

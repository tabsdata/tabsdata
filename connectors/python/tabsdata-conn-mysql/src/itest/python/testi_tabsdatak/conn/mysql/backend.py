#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, text
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain
from tabsdatax.itest.fixture.backends.mysqlcdc import MysqlcdcDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mysql import (
    GtidPosition,
    MySQLCdcStreamSrcConn,
    MySQLDestConn,
    MySQLSrcConn,
)

# MySQL quotes identifiers with backticks.
DELIMITERS = ("`", "`")


def src_conn(domain: MysqlDomain) -> MySQLSrcConn:
    return MySQLSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: MysqlDomain) -> MySQLDestConn:
    return MySQLDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: MysqlDomain | MysqlcdcDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)


def cdc_src_conn(domain: MysqlcdcDomain) -> MySQLCdcStreamSrcConn:
    """The connection a CDC stream source reads the binlog through. The domain owner
    carries the REPLICATION SLAVE / CLIENT grants the stream needs; the connector's
    default ``server_id`` (512) is free, the server itself being 1."""
    return MySQLCdcStreamSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def no_lock_src_conn(domain: MysqlcdcDomain) -> MySQLCdcStreamSrcConn:
    """The connection of the domain's user without RELOAD: the server refuses it
    the global read lock, as managed services such as Amazon RDS refuse it to
    every user, so an initial load through it reads a deduplicated window."""
    if not domain.no_lock_user:
        pytest.fail(
            "the mysqlcdc domain predates its user without RELOAD; create it again"
        )
    return MySQLCdcStreamSrcConn(
        uri=domain.uri,
        credentials=UserPassword(
            user=Secret(domain.no_lock_user), password=Secret(domain.password)
        ),
    )


def cdc_table(domain: MysqlcdcDomain, table: str) -> str:
    """`table` as a CDC source tracks it: mysql qualifies with the database."""
    return f"{domain.database}.{table}"


def cdc_anchor(domain: MysqlcdcDomain) -> GtidPosition:
    """The binlog position everything committed so far is behind, as a ``start_from``.
    Anchoring there is what makes a capture assertion deterministic: the events the
    test then produces are replayed whenever the stream attaches, and nothing from an
    earlier run (this binlog outlives the test) can come with them."""
    engine = create_engine(domain.sqlalchemy_uri)
    try:
        with engine.connect() as conn:
            return GtidPosition(
                gtid=conn.execute(text("SELECT @@gtid_executed")).scalar()
            )
    finally:
        engine.dispose()


class CdcProbe:
    """A two-column table whose INSERT / UPDATE / DELETE are the change events a CDC
    test captures. The DML is the test's subject, so it runs straight over the domain's
    credential-bearing URI rather than through the connector."""

    def __init__(self, domain: MysqlcdcDomain, table: str) -> None:
        self._engine = create_engine(domain.sqlalchemy_uri)
        self._quoted = f"`{table}`"
        self.name = table

    def _execute(self, statement: str, **binds) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(statement), binds)

    def create(self) -> None:
        self._execute(f"DROP TABLE IF EXISTS {self._quoted}")
        self._execute(
            f"CREATE TABLE {self._quoted} "
            f"(id INTEGER PRIMARY KEY, name VARCHAR(64) NOT NULL)"
        )

    def drop(self) -> None:
        self._execute(f"DROP TABLE IF EXISTS {self._quoted}")
        self._engine.dispose()

    def insert(self, id_: int, name: str) -> None:
        self._execute(
            f"INSERT INTO {self._quoted} (id, name) VALUES (:id, :name)",
            id=id_,
            name=name,
        )

    def update(self, id_: int, name: str) -> None:
        self._execute(
            f"UPDATE {self._quoted} SET name = :name WHERE id = :id", id=id_, name=name
        )

    def delete(self, id_: int) -> None:
        self._execute(f"DELETE FROM {self._quoted} WHERE id = :id", id=id_)

    def truncate(self) -> None:
        self._execute(f"TRUNCATE TABLE {self._quoted}")


@contextmanager
def cdc_probe(domain: MysqlcdcDomain, table: str) -> Iterator[CdcProbe]:
    """A :class:`CdcProbe` on `table`, created before the test anchors the stream (the
    source harvests the tracked table's schema when it attaches) and dropped after, so
    the domain is left as it was found."""
    probe = CdcProbe(domain, table)
    probe.create()
    try:
        yield probe
    finally:
        probe.drop()

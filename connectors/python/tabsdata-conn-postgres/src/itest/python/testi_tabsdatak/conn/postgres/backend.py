#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain
from tabsdatax.itest.fixture.backends.postgrescdc import PostgrescdcDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.postgres import (
    PostgresCdcStreamSrcConn,
    PostgresDestConn,
    PostgresSrcConn,
)

# Postgres quotes identifiers with double quotes.
DELIMITERS = ('"', '"')

# tdx gives each domain its own database, so its entities live in that database's
# default schema.
CDC_SCHEMA = "public"

WAL2JSON = "wal2json"


def src_conn(domain: PostgresDomain) -> PostgresSrcConn:
    return PostgresSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: PostgresDomain) -> PostgresDestConn:
    return PostgresDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: PostgresDomain | PostgrescdcDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)


def cdc_src_conn(domain: PostgrescdcDomain) -> PostgresCdcStreamSrcConn:
    """The connection a CDC stream source decodes the WAL through. The domain owner
    carries the REPLICATION attribute a logical-replication connection needs."""
    return PostgresCdcStreamSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def cdc_table(table: str) -> str:
    """`table` as a CDC source tracks it: postgres qualifies with the schema."""
    return f"{CDC_SCHEMA}.{table}"


@contextmanager
def replication_slot(domain: PostgrescdcDomain, name: str) -> Iterator[str]:
    """A wal2json logical slot for the CDC source to attach to, created before the test
    produces anything (the slot's creation point is the earliest WAL it can replay, so
    creating it first is what makes a capture assertion deterministic) and dropped
    after. A slot left behind pins every WAL segment after its restart position and
    would replay a previous run's changes. Slot functions cannot run inside a
    transaction block, hence AUTOCOMMIT."""
    engine = create_engine(domain.sqlalchemy_uri, isolation_level="AUTOCOMMIT")
    try:
        _drop_replication_slot(engine, name)
        with engine.connect() as conn:
            conn.execute(
                text("SELECT pg_create_logical_replication_slot(:slot, :plugin)"),
                {"slot": name, "plugin": WAL2JSON},
            )
        try:
            yield name
        finally:
            _drop_replication_slot(engine, name)
    finally:
        engine.dispose()


@contextmanager
def free_replication_slot(domain: PostgrescdcDomain, name: str) -> Iterator[str]:
    """`name` guaranteed free for an initial load to create itself, and dropped after.

    An initial load exports its snapshot from ``CREATE_REPLICATION_SLOT ...
    EXPORT_SNAPSHOT``, which only exports at creation, so the slot must not already
    exist -- the source rejects one that does. The snapshot is what makes the assertion
    deterministic here, in place of the pre-created slot :func:`replication_slot` gives
    a plain capture test."""
    engine = create_engine(domain.sqlalchemy_uri, isolation_level="AUTOCOMMIT")
    try:
        _drop_replication_slot(engine, name)
        try:
            yield name
        finally:
            _drop_replication_slot(engine, name)
    finally:
        engine.dispose()


def drop_replication_slot(domain: PostgrescdcDomain, name: str) -> None:
    """Drop `name` if it exists, the way an operator does before forcing a fresh
    initial load: the load exports its snapshot at slot creation, so a second one over
    a live slot is refused."""
    engine = create_engine(domain.sqlalchemy_uri, isolation_level="AUTOCOMMIT")
    try:
        _drop_replication_slot(engine, name)
    finally:
        engine.dispose()


def _drop_replication_slot(engine, name: str) -> None:
    # The source closes its replication connection client-side, but the server-side
    # walsender can still hold the slot, and a held slot cannot be dropped.
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT active_pid FROM pg_replication_slots WHERE slot_name = :slot"),
            {"slot": name},
        ).fetchone()
        if row is None:
            return
        if row[0] is not None:
            conn.execute(text("SELECT pg_terminate_backend(:pid)"), {"pid": row[0]})
        conn.execute(text("SELECT pg_drop_replication_slot(:slot)"), {"slot": name})


class CdcProbe:
    """A two-column table whose INSERT / UPDATE / DELETE are the change events a CDC
    test captures. The DML is the test's subject, so it runs straight over the domain's
    credential-bearing URI rather than through the connector."""

    def __init__(self, domain: PostgrescdcDomain, table: str) -> None:
        self._engine = create_engine(domain.sqlalchemy_uri)
        self._quoted = f'"{table}"'
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
        # Without FULL, an UPDATE / DELETE change record carries the key columns only,
        # so it would have no before-image.
        self._execute(f"ALTER TABLE {self._quoted} REPLICA IDENTITY FULL")

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
def cdc_probe(domain: PostgrescdcDomain, table: str) -> Iterator[CdcProbe]:
    """A :class:`CdcProbe` on `table`, created before the test attaches the stream (the
    source harvests the tracked table's schema when it attaches) and dropped after, so
    the domain is left as it was found."""
    probe = CdcProbe(domain, table)
    probe.create()
    try:
        yield probe
    finally:
        probe.drop()

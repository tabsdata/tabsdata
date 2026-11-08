#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

import pytest
from sqlalchemy import Engine, create_engine, text
from tabsdatax.itest.fixture.backends.db2 import Db2Domain
from tabsdatax.itest.fixture.backends.db2cdc import Db2CdcDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.db2 import Db2CdcStreamSrcConn, Db2DestConn, Db2SrcConn

# DB2 folds unquoted identifiers to upper case, and both the provisioned tables
# and the ones the connector creates are unquoted -- so the store quotes nothing
# and lets that folding line the names up. Quoting a lower-case name would force
# a case-sensitive match that never resolves.
DELIMITERS = ("", "")

logger = logging.getLogger(__name__)


def src_conn(domain: Db2Domain) -> Db2SrcConn:
    return Db2SrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: Db2Domain) -> Db2DestConn:
    return Db2DestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: Db2Domain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI (it already carries the ``?currentSchema=`` the
    ``db2+ibm_db`` dialect needs)."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)


# --- CDC ---------------------------------------------------------------
#
# The db2cdc domain owns the replication apparatus: archive logging, the
# ``ASN.IBMSNAP_*`` control tables, the running ``asncap`` capture program and
# per-table registration. A test asks it for a source table and drives real
# INSERT / UPDATE / DELETE against it; asncap propagates each one into the CD
# table on its own schedule, which is why every assertion here polls.


def cdc_src_conn(domain: Db2CdcDomain) -> Db2CdcStreamSrcConn:
    """The connection a CDC stream source reads the CD tables through."""
    return Db2CdcStreamSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def cdc_engine(domain: Db2CdcDomain) -> Engine:
    return create_engine(domain.sqlalchemy_uri)


CDC_SOURCE_COLUMNS = "ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(64), AGE INTEGER"


@contextmanager
def cdc_source_table(
    domain: Db2CdcDomain, name: str | None = None, columns: str = CDC_SOURCE_COLUMNS
) -> Iterator[tuple[str, str]]:
    """A freshly captured CDC source table, yielded as ``(table, cd_table)``.

    Capture is the domain's (``tdd domain capture-table``), so the six-step
    ``asnclp`` sequence lives in one place. The name is unique per call unless
    one is given: a CD table keeps its rows until asncap prunes them, so two
    tests sharing one source table would see each other's events.
    """
    table = (name or f"TD_CDC_{uuid.uuid4().hex[:12]}").upper()
    schema = domain.schema.upper()
    ddl = f"create table {schema}.{table} ({columns})"
    if name:
        # A generated name is new every time, but a fixed one may survive a run
        # that died before its teardown.
        domain.release_table(table=table, drop=True)
    cd_table = domain.capture_table(table=table, ddl=ddl)
    try:
        yield table, cd_table
    finally:
        _release_after_use(domain, table)


# tdd leaves the registration, the CD table and the source table in place when
# asncap runs but has not taken the stop yet; its error carries no code.
_CAPTURE_BUSY = "has not taken the CAPSTOP"


def _release_after_use(domain: Db2CdcDomain, table: str) -> None:
    """Release a table a test is done with. A table asncap is still behind on
    stays: no other test uses its name, and the domain's next destroy or model
    install removes it. Every other failure fails the test."""
    try:
        domain.release_table(table=table, drop=True)
    except Exception as error:
        if _CAPTURE_BUSY not in str(error):
            raise
        logger.warning("Left %s registered for capture: %s", table, error)


def cdc_insert(domain: Db2CdcDomain, table: str, rows: list[dict]) -> None:
    engine = cdc_engine(domain)
    try:
        with engine.begin() as conn:
            for row in rows:
                conn.execute(
                    text(
                        f"insert into {domain.schema.upper()}.{table} "
                        f"(id, name, age) values (:id, :name, :age)"
                    ),
                    row,
                )
    finally:
        engine.dispose()


def cdc_update(domain: Db2CdcDomain, table: str, id_: int, name: str, age: int) -> None:
    engine = cdc_engine(domain)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"update {domain.schema.upper()}.{table} "
                    f"set name = :name, age = :age where id = :id"
                ),
                {"name": name, "age": age, "id": id_},
            )
    finally:
        engine.dispose()


def cdc_delete(domain: Db2CdcDomain, table: str, id_: int) -> None:
    engine = cdc_engine(domain)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(f"delete from {domain.schema.upper()}.{table} where id = :id"),
                {"id": id_},
            )
    finally:
        engine.dispose()


def cdc_wait_cd_count(
    domain: Db2CdcDomain,
    cd_table: str,
    expected: int,
    timeout_s: float = 120.0,
    poll_s: float = 2.0,
) -> None:
    """Block until asncap has propagated at least `expected` rows into
    `cd_table`.

    The CD table fills asynchronously after the source COMMIT: asncap reads the
    archived log, transforms each row and writes it to the staging table. Local
    runs land within a few seconds to half a minute.
    """
    engine = cdc_engine(domain)
    deadline = time.monotonic() + timeout_s
    seen = -1
    try:
        while time.monotonic() < deadline:
            with engine.connect() as conn:
                seen = int(
                    conn.execute(text(f"select count(*) from {cd_table}")).scalar() or 0
                )
            if seen >= expected:
                return
            time.sleep(poll_s)
    finally:
        engine.dispose()
    pytest.fail(
        f"asncap did not propagate {expected} row(s) into {cd_table} within "
        f"{timeout_s:.0f}s (saw {seen})"
    )


def cdc_commitseqs(domain: Db2CdcDomain, cd_table: str) -> list[bytes]:
    """The CD table's commitseqs in arrival order, so a test can pick a real
    watermark mid-stream."""
    engine = cdc_engine(domain)
    try:
        with engine.connect() as conn:
            return [
                bytes(r[0])
                for r in conn.execute(
                    text(
                        f"select IBMSNAP_COMMITSEQ from {cd_table} "
                        f"order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ"
                    )
                ).fetchall()
            ]
    finally:
        engine.dispose()

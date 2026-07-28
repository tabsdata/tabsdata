#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from tabsdatax.itest.fixture.backends.mssqlserver import MssqlserverDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.mssql import MSSQLDestConn, MSSQLSrcConn

# SQL Server quotes identifiers with brackets.
DELIMITERS = ("[", "]")


def src_conn(domain: MssqlserverDomain) -> MSSQLSrcConn:
    return MSSQLSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: MssqlserverDomain) -> MSSQLDestConn:
    return MSSQLDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: MssqlserverDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)

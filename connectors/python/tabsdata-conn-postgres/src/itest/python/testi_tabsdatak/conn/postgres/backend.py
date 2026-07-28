#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from tabsdatax.itest.fixture.backends.postgres import PostgresDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.postgres import PostgresDestConn, PostgresSrcConn

# Postgres quotes identifiers with double quotes.
DELIMITERS = ('"', '"')


def src_conn(domain: PostgresDomain) -> PostgresSrcConn:
    return PostgresSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: PostgresDomain) -> PostgresDestConn:
    return PostgresDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: PostgresDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)

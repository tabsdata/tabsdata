#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from tabsdatax.itest.fixture.backends.mysql import MysqlDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.mysql import MySQLDestConn, MySQLSrcConn

# MySQL quotes identifiers with backticks.
DELIMITERS = ("`", "`")


def src_conn(domain: MysqlDomain) -> MySQLSrcConn:
    return MySQLSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: MysqlDomain) -> MySQLDestConn:
    return MySQLDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: MysqlDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)

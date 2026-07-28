#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from tabsdatax.itest.fixture.backends.db2 import Db2Domain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.db2 import Db2DestConn, Db2SrcConn

# DB2 folds unquoted identifiers to upper case, and both the provisioned tables
# and the ones the connector creates are unquoted -- so the store quotes nothing
# and lets that folding line the names up. Quoting a lower-case name would force
# a case-sensitive match that never resolves.
DELIMITERS = ("", "")


def src_conn(domain: Db2Domain) -> Db2SrcConn:
    return Db2SrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: Db2Domain) -> Db2DestConn:
    return Db2DestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: Db2Domain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI (it already carries the ``?currentSchema=`` the
    ``db2+ibm_db`` dialect needs)."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)

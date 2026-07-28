#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

from tabsdatax.itest.fixture.backends.oracle import OracleDomain
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore, sql_credentials

from tabsdatak.conn.oracle import OracleDestConn, OracleSrcConn

# Oracle folds unquoted identifiers to upper case, and both the provisioned tables
# and those the connector creates are made unquoted -- so the store quotes nothing
# and lets that folding line the names up. Quoting a lower-case literal would ask
# for a case-sensitive match that never resolves (ORA-00942).
DELIMITERS = ("", "")


def src_conn(domain: OracleDomain) -> OracleSrcConn:
    return OracleSrcConn(uri=domain.uri, credentials=sql_credentials(domain))


def dest_conn(domain: OracleDomain) -> OracleDestConn:
    return OracleDestConn(uri=domain.uri, credentials=sql_credentials(domain))


def store(domain: OracleDomain) -> SqlAlchemyStore:
    """A store to read back / drop what a test wrote, over the domain's own
    credential-bearing URI."""
    return SqlAlchemyStore(domain.sqlalchemy_uri, delimiters=DELIMITERS)

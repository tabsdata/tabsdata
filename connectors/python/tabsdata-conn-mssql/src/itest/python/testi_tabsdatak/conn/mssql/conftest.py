#
# Copyright 2026 Tabsdata Inc.
#

"""Shared MSSQL test fixtures for this package's integration and e2e suites: a
``SqlAlchemyStore`` over whichever mssqlserver backend the test declared (its
``domain``) to read tables back, drop scratch tables, and stage a deterministic
source table. The shared SQL layer lives in
:mod:`tabsdatax.itest.fixture.functions.sql`; only the mssql URI (pyodbc + ODBC
Driver 18) and identifier delimiters are here."""

from __future__ import annotations

import urllib.parse

import pytest
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore

_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


def _uri(backend) -> str:
    user = urllib.parse.quote(backend.user, safe="")
    password = urllib.parse.quote(backend.password, safe="")
    driver = urllib.parse.quote_plus(_ODBC_DRIVER)
    return (
        f"mssql+pyodbc://{user}:{password}@{backend.host}:{backend.port}/{backend.db}"
        f"?driver={driver}&TrustServerCertificate=yes"
    )


@pytest.fixture
def sql():
    """Factory building a ``SqlStore`` over the mssqlserver backend the test
    declared -- call it with ``domain_mssql``."""
    return lambda backend: SqlAlchemyStore(_uri(backend), delimiters=("[", "]"))

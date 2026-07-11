#
# Copyright 2026 Tabsdata Inc.
#

"""Shared MySQL test fixtures for this package's integration and e2e suites: a
``SqlAlchemyStore`` over whichever mysql backend the test declared (its
``domain``) to read tables back, drop scratch tables, and stage a deterministic
source table. The shared SQL layer lives in
:mod:`tabsdatax.itest.fixture.functions.sql`; only the mysql URI and identifier
delimiters are here."""

from __future__ import annotations

import urllib.parse

import pytest
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore


def _uri(backend) -> str:
    user = urllib.parse.quote(backend.user, safe="")
    password = urllib.parse.quote(backend.password, safe="")
    return (
        f"mysql+pymysql://{user}:{password}@{backend.host}:{backend.port}/{backend.db}"
    )


@pytest.fixture
def sql():
    """Factory building a ``SqlStore`` over the mysql backend the test declared
    -- call it with ``domain_mysql``."""
    return lambda backend: SqlAlchemyStore(_uri(backend), delimiters=("`", "`"))

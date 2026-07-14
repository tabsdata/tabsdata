#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Oracle test fixtures for this package's integration and e2e suites: a
``SqlAlchemyStore`` over whichever oracle backend the test declared (its
``domain``) to read tables back, drop scratch tables, and stage a deterministic
source table. The shared SQL layer lives in
:mod:`tabsdatax.itest.fixture.functions.sql`; only the oracle connection is here.

Oracle's ``sqlalchemy_uri`` already carries credentials and the
``?service_name=FREEPDB1`` the ``oracle+oracledb`` dialect needs, so the store
uses it directly rather than rebuilding it from host/port/db.

Unlike the other SQL dialects, the store uses **empty** delimiters (no-op
``quote()``). Oracle folds unquoted identifiers to upper case, and both the
provisioned tables and those polars writes through the connector are created
unquoted -- so they live under upper-case names. Quoting a lower-case name would
force a case-sensitive match that never resolves (``ORA-00942``); leaving names
bare lets Oracle's own folding line them up."""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore


@pytest.fixture
def sql():
    """Factory building a ``SqlStore`` over the oracle backend the test declared
    -- call it with ``domain_oracle``."""
    return lambda backend: SqlAlchemyStore(backend.sqlalchemy_uri, delimiters=("", ""))

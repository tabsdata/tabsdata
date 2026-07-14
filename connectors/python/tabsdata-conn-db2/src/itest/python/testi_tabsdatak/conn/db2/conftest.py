#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Db2 test fixtures for this package's integration and e2e suites: a
``SqlAlchemyStore`` over whichever db2 backend the test declared (its ``domain``)
to read tables back, drop scratch tables, and stage a deterministic source table.
The shared SQL layer lives in :mod:`tabsdatax.itest.fixture.functions.sql`; only
the db2 connection is here.

Db2's ``sqlalchemy_uri`` already carries credentials and the ``?currentSchema=``
the ``db2+ibm_db`` dialect needs, so the store uses it directly rather than
rebuilding it from host/port/db.

Like Oracle, the store uses **empty** delimiters (no-op ``quote()``). Db2 folds
unquoted identifiers to upper case, and both the provisioned tables and those
polars writes through the connector are created unquoted -- so they live under
upper-case names. Quoting a lower-case name would force a case-sensitive match
that never resolves; leaving names bare lets Db2's own folding line them up."""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.functions.sql import SqlAlchemyStore


@pytest.fixture
def sql():
    """Factory building a ``SqlStore`` over the db2 backend the test declared --
    call it with ``domain_db2``."""
    return lambda backend: SqlAlchemyStore(backend.sqlalchemy_uri, delimiters=("", ""))

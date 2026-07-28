#
# Copyright 2026 Tabsdata Inc.
#

"""DB2 explorer integration tests against a provisioned domain"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENT_ROWS,
    DEPARTMENTS,
    EMPLOYEES,
    EMPLOYEES_COLUMNS,
)
from testi_tabsdatak.conn.db2.backend import src_conn

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.db2 import Db2SrcConn
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer

pytestmark = pytest.mark.db2


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@db2_domain("db2_employees_xs")
def test_check_connect_succeeds(
    domain_db2_employees_xs: Db2Domain,
):
    conn = src_conn(domain_db2_employees_xs)
    assert Db2ConnectionExplorer().check_connect(conn) is None


@db2_domain("db2_employees_xs")
def test_check_connect_bad_credentials_raises(
    domain_db2_employees_xs: Db2Domain,
):
    bad = Db2SrcConn(
        uri=domain_db2_employees_xs.uri,
        credentials=UserPassword(
            user=Secret(domain_db2_employees_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # A failed DB2 logon surfaces as an ibm_db error wrapped by SQLAlchemy
    # (DBAPIError is the common base of those).
    with pytest.raises(DBAPIError):
        Db2ConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@db2_domain("db2_employees_xs")
def test_metadata_lists_provisioned_tables(
    domain_db2_employees_xs: Db2Domain,
):
    domain = domain_db2_employees_xs
    conn = src_conn(domain)
    entities = Db2ConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is restricted to the domain's own schema (the URI's
    # ``currentSchema``), so exactly the provisioned tables show up -- DB2 stores
    # them upper-cased but ibm_db_sa normalizes reflected names back to lower case.
    assert {e.table for e in entities} == set(domain.entities)

    # No `map_entity_fields` override, so the base maps the iterated schema name
    # into both `database` and `schema`.
    departments = _entity_by_table(entities, DEPARTMENTS)
    assert departments.database == domain.schema.upper()
    assert departments.schema == domain.schema.upper()


@db2_domain("db2_employees_xs")
def test_metadata_reflects_columns_and_primary_key(
    domain_db2_employees_xs: Db2Domain,
):
    conn = src_conn(domain_db2_employees_xs)
    employees = _entity_by_table(Db2ConnectionExplorer().metadata(conn), EMPLOYEES)

    by_name = {c.name: c for c in employees.columns}
    assert set(by_name) == EMPLOYEES_COLUMNS

    # `emp_no` is the sole primary key.
    assert by_name["emp_no"].primary_key is True
    assert not any(
        column.primary_key for name, column in by_name.items() if name != "emp_no"
    )

    # Types are reflected and simplified to a short canonical name.
    assert by_name["emp_no"].type.startswith("INT")
    assert by_name["birth_date"].type == "DATE"
    assert by_name["first_name"].type == "VARCHAR"
    assert by_name["last_name"].type == "VARCHAR"
    assert by_name["gender"].type == "CHAR"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@db2_domain("db2_employees_xs")
def test_data_returns_all_rows(
    domain_db2_employees_xs: Db2Domain,
):
    conn = src_conn(domain_db2_employees_xs)
    explorer = Db2ConnectionExplorer()
    departments = _entity_by_table(explorer.metadata(conn), DEPARTMENTS)

    rows = explorer.data(conn, departments, limit=3)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"dept_no", "dept_name"} for r in rows)
    # `data` applies no ordering, so a limited read is asserted against the
    # model's fixed reference rows rather than against a prefix of them.
    got = {r["dept_no"]: r["dept_name"] for r in rows}
    assert len(got) == 3
    assert got.items() <= DEPARTMENT_ROWS.items()


@db2_domain("db2_employees_xs")
def test_data_respects_limit(
    domain_db2_employees_xs: Db2Domain,
):
    conn = src_conn(domain_db2_employees_xs)
    explorer = Db2ConnectionExplorer()
    employees = _entity_by_table(explorer.metadata(conn), EMPLOYEES)

    rows = explorer.data(conn, employees, limit=2)
    assert len(rows) == 2

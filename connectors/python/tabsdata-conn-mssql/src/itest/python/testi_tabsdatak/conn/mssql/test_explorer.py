#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENT_ROWS,
    DEPARTMENTS,
    EMPLOYEES,
    EMPLOYEES_COLUMNS,
)
from testi_tabsdatak.conn.mssql.backend import src_conn

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.mssql import MSSQLSrcConn
from tabsdatak.conn.mssql._explorer import MSSQLServerExplorer

pytestmark = pytest.mark.mssql


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@mssqlserver_domain("mssqlserver_employees_xs")
def test_check_connect_succeeds(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    conn = src_conn(domain_mssqlserver_employees_xs)
    assert MSSQLServerExplorer().check_connect(conn) is None


@mssqlserver_domain("mssqlserver_employees_xs")
def test_check_connect_bad_credentials_raises(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    bad = MSSQLSrcConn(
        uri=domain_mssqlserver_employees_xs.uri,
        credentials=UserPassword(
            user=Secret(domain_mssqlserver_employees_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # A failed SQL Server logon surfaces as a pyodbc error wrapped by
    # SQLAlchemy (DBAPIError is the common base of those).
    with pytest.raises(DBAPIError):
        MSSQLServerExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@mssqlserver_domain("mssqlserver_employees_xs")
def test_metadata_lists_provisioned_tables(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    domain = domain_mssqlserver_employees_xs
    conn = src_conn(domain)
    entities = MSSQLServerExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only ``dbo`` holds user tables; sys / INFORMATION_SCHEMA and the fixed
    # database roles are skipped, so exactly the provisioned tables show up.
    assert {e.table for e in entities} == set(domain.entities)

    # No `map_entity_fields` override, so the base maps the iterated schema
    # name (``dbo``) into both `database` and `schema`.
    departments = _entity_by_table(entities, DEPARTMENTS)
    assert departments.database == "dbo"
    assert departments.schema == "dbo"


@mssqlserver_domain("mssqlserver_employees_xs")
def test_metadata_reflects_columns_and_primary_key(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    conn = src_conn(domain_mssqlserver_employees_xs)
    employees = _entity_by_table(MSSQLServerExplorer().metadata(conn), EMPLOYEES)

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


@mssqlserver_domain("mssqlserver_employees_xs")
def test_data_returns_all_rows(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    conn = src_conn(domain_mssqlserver_employees_xs)
    explorer = MSSQLServerExplorer()
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


@mssqlserver_domain("mssqlserver_employees_xs")
def test_data_respects_limit(
    domain_mssqlserver_employees_xs: MssqlserverDomain,
):
    conn = src_conn(domain_mssqlserver_employees_xs)
    explorer = MSSQLServerExplorer()
    employees = _entity_by_table(explorer.metadata(conn), EMPLOYEES)

    rows = explorer.data(conn, employees, limit=2)
    assert len(rows) == 2

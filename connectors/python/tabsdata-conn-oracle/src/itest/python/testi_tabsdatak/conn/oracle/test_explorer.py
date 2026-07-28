#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle explorer integration tests against a provisioned domain"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DatabaseError
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENT_ROWS,
    DEPARTMENTS,
    EMPLOYEES,
    EMPLOYEES_COLUMNS,
)
from testi_tabsdatak.conn.oracle.backend import src_conn

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.oracle import OracleSrcConn
from tabsdatak.conn.oracle._explorer import OracleConnectionExplorer

pytestmark = pytest.mark.oracle


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@oracle_domain("oracle_employees_xs")
def test_check_connect_succeeds(
    domain_oracle_employees_xs: OracleDomain,
):
    conn = src_conn(domain_oracle_employees_xs)
    assert OracleConnectionExplorer().check_connect(conn) is None


@oracle_domain("oracle_employees_xs")
def test_check_connect_bad_credentials_raises(
    domain_oracle_employees_xs: OracleDomain,
):
    bad = OracleSrcConn(
        uri=domain_oracle_employees_xs.uri,
        credentials=UserPassword(
            user=Secret(domain_oracle_employees_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # A failed Oracle logon surfaces as an oracledb error wrapped by SQLAlchemy
    # (ORA-01017 arrives as a DatabaseError).
    with pytest.raises(DatabaseError):
        OracleConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@oracle_domain("oracle_employees_xs")
def test_metadata_lists_provisioned_tables(
    domain_oracle_employees_xs: OracleDomain,
):
    domain = domain_oracle_employees_xs
    conn = src_conn(domain)
    entities = OracleConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only the connecting user's schema holds tables, and the oracledb dialect
    # normalizes Oracle's upper-cased names back to lower case on reflection, so
    # exactly the provisioned tables show up under the names the descriptor lists.
    assert {e.table for e in entities} == set(domain.entities)

    # `map_entity_fields` leaves `database` blank and maps the iterated schema name
    # (the owner) into `schema`.
    departments = _entity_by_table(entities, DEPARTMENTS)
    assert departments.database == ""
    assert departments.schema == domain.user


@oracle_domain("oracle_employees_xs")
def test_metadata_reflects_columns_and_primary_key(
    domain_oracle_employees_xs: OracleDomain,
):
    conn = src_conn(domain_oracle_employees_xs)
    employees = _entity_by_table(OracleConnectionExplorer().metadata(conn), EMPLOYEES)

    by_name = {c.name: c for c in employees.columns}
    assert set(by_name) == EMPLOYEES_COLUMNS

    # `emp_no` is the sole primary key.
    assert by_name["emp_no"].primary_key is True
    assert not any(
        column.primary_key for name, column in by_name.items() if name != "emp_no"
    )

    # Types are reflected and simplified to a short canonical name; Oracle has one
    # numeric type, so the integer key comes back as NUMBER.
    assert by_name["emp_no"].type == "NUMBER"
    assert by_name["birth_date"].type == "DATE"
    assert by_name["first_name"].type == "VARCHAR"
    assert by_name["last_name"].type == "VARCHAR"
    assert by_name["gender"].type == "CHAR"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@oracle_domain("oracle_employees_xs")
def test_data_returns_all_rows(
    domain_oracle_employees_xs: OracleDomain,
):
    conn = src_conn(domain_oracle_employees_xs)
    explorer = OracleConnectionExplorer()
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


@oracle_domain("oracle_employees_xs")
def test_data_respects_limit(
    domain_oracle_employees_xs: OracleDomain,
):
    conn = src_conn(domain_oracle_employees_xs)
    explorer = OracleConnectionExplorer()
    employees = _entity_by_table(explorer.metadata(conn), EMPLOYEES)

    rows = explorer.data(conn, employees, limit=2)
    assert len(rows) == 2

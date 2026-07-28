#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from sqlalchemy.exc import OperationalError
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENT_ROWS,
    DEPARTMENTS,
    EMPLOYEES,
    EMPLOYEES_COLUMNS,
)
from testi_tabsdatak.conn.postgres.backend import src_conn

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.postgres import PostgresSrcConn
from tabsdatak.conn.postgres._explorer import PostgresConnectionExplorer

pytestmark = pytest.mark.postgres


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@postgres_domain("postgres_employees_xs")
def test_check_connect_succeeds(
    domain_postgres_employees_xs: PostgresDomain,
):
    conn = src_conn(domain_postgres_employees_xs)
    assert PostgresConnectionExplorer().check_connect(conn) is None


@postgres_domain("postgres_employees_xs")
def test_check_connect_bad_credentials_raises(
    domain_postgres_employees_xs: PostgresDomain,
):
    bad = PostgresSrcConn(
        uri=domain_postgres_employees_xs.uri,
        credentials=UserPassword(
            user=Secret(domain_postgres_employees_xs.user),
            password=Secret("definitely-wrong-password"),
        ),
    )
    # A failed postgres logon surfaces as a psycopg2 error wrapped by
    # SQLAlchemy (OperationalError is what a rejected password raises).
    with pytest.raises(OperationalError):
        PostgresConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@postgres_domain("postgres_employees_xs")
def test_metadata_lists_provisioned_tables(
    domain_postgres_employees_xs: PostgresDomain,
):
    domain = domain_postgres_employees_xs
    conn = src_conn(domain)
    entities = PostgresConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # Only ``public`` holds user tables; pg_catalog / information_schema / pg_toast
    # are skipped, so exactly the provisioned tables show up.
    assert {e.table for e in entities} == set(domain.entities)

    # `map_entity_fields` maps the URI-path database into `database` and the
    # iterated schema name into `schema`.
    departments = _entity_by_table(entities, DEPARTMENTS)
    assert departments.database == domain.db
    assert departments.schema == "public"


@postgres_domain("postgres_employees_xs")
def test_metadata_reflects_columns_and_primary_key(
    domain_postgres_employees_xs: PostgresDomain,
):
    conn = src_conn(domain_postgres_employees_xs)
    employees = _entity_by_table(PostgresConnectionExplorer().metadata(conn), EMPLOYEES)

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


@postgres_domain("postgres_employees_xs")
def test_data_returns_all_rows(
    domain_postgres_employees_xs: PostgresDomain,
):
    conn = src_conn(domain_postgres_employees_xs)
    explorer = PostgresConnectionExplorer()
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


@postgres_domain("postgres_employees_xs")
def test_data_respects_limit(
    domain_postgres_employees_xs: PostgresDomain,
):
    conn = src_conn(domain_postgres_employees_xs)
    explorer = PostgresConnectionExplorer()
    employees = _entity_by_table(explorer.metadata(conn), EMPLOYEES)

    rows = explorer.data(conn, employees, limit=2)
    assert len(rows) == 2

#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from databricks.sdk.errors import DatabricksError
from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    databricks_domain,
)
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENT_ROWS,
    DEPARTMENTS,
    EMPLOYEES,
    EMPLOYEES_COLUMNS,
)
from testi_tabsdatak.conn.databricks.backend import src_conn

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.databricks import DatabricksSrcConn
from tabsdatak.conn.databricks._explorer import DatabricksConnectionExplorer

pytestmark = pytest.mark.databricks


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@databricks_domain("databricks_employees_xs")
def test_check_connect_succeeds(
    domain_databricks_employees_xs: DatabricksDomain,
):
    conn = src_conn(domain_databricks_employees_xs)
    assert DatabricksConnectionExplorer().check_connect(conn) is None


@databricks_domain("databricks_employees_xs")
def test_check_connect_bad_credentials_raises(
    domain_databricks_employees_xs: DatabricksDomain,
):
    domain = domain_databricks_employees_xs
    bad = DatabricksSrcConn(
        host_url=Secret(domain.host),
        token=Secret("definitely-wrong-token"),
        volume=Secret(domain.volume),
        warehouse_id=Secret(domain.warehouse_id),
        catalog=Secret(domain.catalog),
        schema=Secret(domain.schema),
    )
    # A rejected PAT surfaces as a databricks-sdk error (DatabricksError is the
    # common base of those).
    with pytest.raises(DatabricksError):
        DatabricksConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@databricks_domain("databricks_employees_xs")
def test_metadata_lists_provisioned_tables(
    domain_databricks_employees_xs: DatabricksDomain,
):
    domain = domain_databricks_employees_xs
    conn = src_conn(domain)
    entities = DatabricksConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SQLEntity) for e in entities)

    # The scan is pinned to the connection's catalog and restricted to its schema,
    # which the domain owns outright, so exactly the provisioned tables show up.
    assert {e.table for e in entities} == set(domain.entities)

    # Unity Catalog's catalog -> schema maps onto the entity's database -> schema.
    departments = _entity_by_table(entities, DEPARTMENTS)
    assert departments.database == domain.catalog
    assert departments.schema == domain.schema


@databricks_domain("databricks_employees_xs")
def test_metadata_reflects_columns(
    domain_databricks_employees_xs: DatabricksDomain,
):
    conn = src_conn(domain_databricks_employees_xs)
    employees = _entity_by_table(
        DatabricksConnectionExplorer().metadata(conn), EMPLOYEES
    )

    by_name = {c.name: c for c in employees.columns}
    assert set(by_name) == EMPLOYEES_COLUMNS

    # Unity Catalog treats primary keys as informational and the seed declares
    # none, so no column reflects as one.
    assert not any(column.primary_key for column in employees.columns)

    # Types are reflected and simplified to a short canonical name.
    assert by_name["emp_no"].type == "INT"
    assert by_name["birth_date"].type == "DATE"
    assert by_name["first_name"].type == "STRING"
    assert by_name["last_name"].type == "STRING"
    assert by_name["gender"].type == "STRING"


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@databricks_domain("databricks_employees_xs")
def test_data_returns_all_rows(
    domain_databricks_employees_xs: DatabricksDomain,
):
    conn = src_conn(domain_databricks_employees_xs)
    explorer = DatabricksConnectionExplorer()
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


@databricks_domain("databricks_employees_xs")
def test_data_respects_limit(
    domain_databricks_employees_xs: DatabricksDomain,
):
    conn = src_conn(domain_databricks_employees_xs)
    explorer = DatabricksConnectionExplorer()
    employees = _entity_by_table(explorer.metadata(conn), EMPLOYEES)

    rows = explorer.data(conn, employees, limit=2)
    assert len(rows) == 2

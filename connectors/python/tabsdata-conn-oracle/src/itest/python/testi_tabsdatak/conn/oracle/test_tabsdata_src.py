#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle source-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    assert_frames_match,
    run_function,
    table_frame,
    wait_for_completion,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.common.sql.employees_model import (
    DEPARTMENTS,
    EMPLOYEES,
    emp_nos,
)
from testi_tabsdatak.conn.common.sql.plugin_io import entity_rows
from testi_tabsdatak.conn.oracle.backend import src_conn, store

from tabsdatak.sdk.status_utils.execution import EXECUTION_SUCCESSFUL_FINAL_STATUSES

pytestmark = pytest.mark.oracle

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")


@tabsdata_instance(consumer="tdtest")
@oracle_domain("oracle_employees_xs")
def test_publisher_reads_table_from_oracle_domain(
    instance_tdtest: Instance,
    domain_oracle_employees_xs: OracleDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_oracle_employees_xs
    expected = store(domain).read_table(DEPARTMENTS)
    # The connector surfaces Oracle's upper-case column names, while the store's
    # SQLAlchemy reads normalize them back to lower case.
    expected = expected.rename({c: c.upper() for c in expected.columns})
    collection = collection_factory("sources", src_conn(domain))
    run_function(
        collection, publisher.pub_publisher_reads_table_from_oracle_domain, FUNCTIONS
    )

    assert_frames_match(table_frame(collection, publisher.PUBLISHED_TABLE), expected)


@tabsdata_instance(consumer="tdtest")
@oracle_domain("oracle_employees_xs")
def test_publisher_initial_values_advance_across_runs(
    instance_tdtest: Instance,
    domain_oracle_employees_xs: OracleDomain,
    collection_factory: CollectionFactory,
) -> None:
    # The publisher seeds its watermark below the first emp_no and advances it to
    # the midpoint via ctx.set_attr, so run 1 reads every row and run 2 only those
    # above the persisted value -- the seed applies once, then the stored value
    # takes over.
    domain = domain_oracle_employees_xs
    all_emp_nos = emp_nos(entity_rows(domain, EMPLOYEES))
    collection = collection_factory("sources", src_conn(domain))
    function = collection.register_function(
        info=publisher.pub_publisher_initial_values_advance_across_runs,
        project_root=str(FUNCTIONS),
    )

    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    first = table_frame(collection, publisher.PUBLISHED_TABLE)
    assert sorted(first["EMP_NO"].to_list()) == all_emp_nos

    status = wait_for_completion(function.trigger())
    assert status in EXECUTION_SUCCESSFUL_FINAL_STATUSES
    second = table_frame(collection, publisher.PUBLISHED_TABLE)
    assert sorted(second["EMP_NO"].to_list()) == [
        n for n in all_emp_nos if n > publisher.WATERMARK_ADVANCE_TO
    ]

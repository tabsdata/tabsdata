#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce source-plugin integration tests using tabsdata"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.salesforce import (
    SalesforceDomain,
    salesforce_domain,
)
from tabsdatax.itest.fixture.functions.harness import (
    CollectionFactory,
    run_function,
    table_frame,
)
from tabsdatax.itest.fixture.instances.tabsdata import Instance, tabsdata_instance
from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.salesforce.backend import sobject_rows, src_conn

pytestmark = pytest.mark.salesforce

# The functions these tests register live in `functions/`, checked in as a user
# project would have them; that directory is the project_root they are bundled
# from.
FUNCTIONS = Path(__file__).parent / "functions"
publisher = function_module(FUNCTIONS, "publisher")


@tabsdata_instance(consumer="tdtest")
@salesforce_domain("salesforce")
def test_publisher_reads_object_from_salesforce_domain(
    instance_tdtest: Instance,
    domain_salesforce: SalesforceDomain,
    collection_factory: CollectionFactory,
) -> None:
    domain = domain_salesforce
    collection = collection_factory("sources", src_conn(domain))
    run_function(
        collection,
        publisher.pub_publisher_reads_object_from_salesforce_domain,
        FUNCTIONS,
    )

    # The org's Account rows are out of the test's control, so what is asserted is
    # the projection and that the SOQL LIMIT survived the round trip. A published
    # table also carries the `$td.*` system columns.
    published = table_frame(collection, publisher.PUBLISHED_TABLE)
    assert {"Id", "Name"} <= set(published.columns)
    assert published.height == min(publisher.ACCOUNT_LIMIT, sobject_rows(domain))

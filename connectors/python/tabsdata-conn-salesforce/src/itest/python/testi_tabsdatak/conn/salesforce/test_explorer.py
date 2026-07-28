#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce connection-explorer integration tests"""

from __future__ import annotations

import pytest
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from tabsdatax.itest.fixture.backends.salesforce import (
    SalesforceDomain,
    salesforce_domain,
)
from testi_tabsdatak.conn.salesforce.backend import ACCOUNT, src_conn, tabular_report

from tabsdatak.api import Secret
from tabsdatak.conn.common._explorer import ObjectsEntity
from tabsdatak.conn.salesforce import SalesforceSrcConn, SalesforceTokenCredentials
from tabsdatak.conn.salesforce._explorer._common import SalesforceReportEntity
from tabsdatak.conn.salesforce._explorer._report_explorer import (
    SalesforceReportsConnectionExplorer,
)
from tabsdatak.conn.salesforce._explorer._soql_explorer import (
    SalesforceSOQLConnectionExplorer,
)

pytestmark = pytest.mark.salesforce


def _report_by_id(
    entities: list[SalesforceReportEntity], report_id: str
) -> SalesforceReportEntity:
    matches = [e for e in entities if e.report_id == report_id]
    assert matches, f"no entity for {report_id!r} in {[e.report_id for e in entities]}"
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@salesforce_domain("salesforce")
def test_check_connect_succeeds(domain_salesforce: SalesforceDomain):
    conn = src_conn(domain_salesforce)
    assert SalesforceSOQLConnectionExplorer().check_connect(conn) is None
    assert SalesforceReportsConnectionExplorer().check_connect(conn) is None


@salesforce_domain("salesforce")
def test_check_connect_bad_credentials_raises(domain_salesforce: SalesforceDomain):
    bad = SalesforceSrcConn(
        credentials=SalesforceTokenCredentials(
            username=Secret(domain_salesforce.username),
            password=Secret("definitely-wrong-password"),
            security_token=Secret(domain_salesforce.token),
        ),
        instance_url=Secret(domain_salesforce.url),
    )
    # The explorer connects through simple-salesforce's own constructor, so a
    # rejected logon surfaces unwrapped.
    with pytest.raises(SalesforceAuthenticationFailed):
        SalesforceSOQLConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@salesforce_domain("salesforce")
def test_metadata_describes_requested_objects(domain_salesforce: SalesforceDomain):
    conn = src_conn(domain_salesforce, objects=[ACCOUNT])
    entities = SalesforceSOQLConnectionExplorer().metadata(conn)

    assert len(entities) == 1
    account = entities[0]
    assert isinstance(account, ObjectsEntity)
    assert account.object == ACCOUNT
    by_name = {p.name: p for p in account.properties}
    assert {"Id", "Name"} <= set(by_name)
    assert by_name["Id"].type == "id"
    assert by_name["Id"].required is True


@salesforce_domain("salesforce")
def test_metadata_without_objects_opens_no_session(
    domain_salesforce: SalesforceDomain,
):
    conn = src_conn(domain_salesforce)
    assert SalesforceSOQLConnectionExplorer().metadata(conn) == []


@salesforce_domain("salesforce")
def test_metadata_skips_unknown_object(domain_salesforce: SalesforceDomain):
    conn = src_conn(domain_salesforce, objects=["NoSuchObject__c", ACCOUNT])
    entities = SalesforceSOQLConnectionExplorer().metadata(conn)
    # A name the org cannot describe is reported and skipped, not fatal.
    assert [e.object for e in entities] == [ACCOUNT]


@salesforce_domain("salesforce")
def test_report_metadata_lists_describable_reports(
    domain_salesforce: SalesforceDomain,
):
    domain = domain_salesforce
    conn = src_conn(domain)
    entities = SalesforceReportsConnectionExplorer().metadata(conn)

    assert entities and all(isinstance(e, SalesforceReportEntity) for e in entities)
    assert all(e.columns for e in entities)

    report = _report_by_id(entities, tabular_report(domain))
    assert report.report


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@salesforce_domain("salesforce")
def test_data_respects_limit(domain_salesforce: SalesforceDomain):
    conn = src_conn(domain_salesforce, objects=[ACCOUNT])
    explorer = SalesforceSOQLConnectionExplorer()
    account = explorer.metadata(conn)[0]

    rows = explorer.data(conn, account, limit=2)

    assert isinstance(rows, list)
    assert len(rows) == 2
    # `data` projects exactly the described fields, whatever the row holds.
    assert all(set(r) == {p.name for p in account.properties} for r in rows)

#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and driver direct access"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.salesforce import SalesforceDomain

from tabsdatak.api import Secret
from tabsdatak.conn.salesforce import (
    ObjectsNameSpec,
    SalesforceSrcConn,
    SalesforceTokenCredentials,
)

# A Salesforce org is a SaaS tenant, not a provisioned schema: the domain carries
# credentials only, so tests read the standard sobject every org holds and assert
# on shape rather than on rows the org owns.
ACCOUNT = "Account"


def src_conn(
    domain: SalesforceDomain, objects: ObjectsNameSpec | None = None
) -> SalesforceSrcConn:
    return SalesforceSrcConn(
        credentials=SalesforceTokenCredentials(
            username=Secret(domain.username),
            password=Secret(domain.password),
            security_token=Secret(domain.token),
        ),
        instance_url=Secret(domain.url),
        objects=objects,
    )


def client(domain: SalesforceDomain):
    """A ``simple_salesforce`` client over the domain's own credentials -- the
    independent oracle a test reads the org back with."""
    from simple_salesforce import Salesforce

    return Salesforce(
        username=domain.username,
        password=domain.password,
        security_token=domain.token,
        instance_url=domain.url,
    )


def sobject_rows(domain: SalesforceDomain, sobject: str = ACCOUNT) -> int:
    return client(domain).query(f"SELECT COUNT() FROM {sobject}")["totalSize"]


def tabular_report(domain: SalesforceDomain) -> str:
    """The id of a Tabular report -- the only format ``SalesforceReportSrc`` reads.
    Report ids are org-specific, so one is discovered instead of pinned."""
    records = client(domain).query(
        "SELECT Id FROM Report WHERE Format = 'Tabular' AND IsDeleted = false "
        "ORDER BY Id LIMIT 1"
    )["records"]
    if not records:
        pytest.skip(f"org {domain.url!r} holds no Tabular report")
    return records[0]["Id"]


def report_columns(
    domain: SalesforceDomain, report_id: str, strategy: str = "columnName"
) -> list[str]:
    """A report's detail columns in order, named per `strategy` -- what the report
    source's parquet is expected to carry, read from the Analytics describe."""
    describe = client(domain).restful(f"analytics/reports/{report_id}/describe")
    info = describe["reportExtendedMetadata"]["detailColumnInfo"]
    return [
        column if strategy == "columnName" else info[column]["label"]
        for column in describe["reportMetadata"]["detailColumns"]
    ]

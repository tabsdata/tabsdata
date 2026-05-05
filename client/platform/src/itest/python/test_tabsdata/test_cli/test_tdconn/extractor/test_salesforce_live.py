#
# Copyright 2026 Tabs Data Inc.
#
"""Live Salesforce extractor tests — gated by the ``SALESFORCE`` env var.

A single Salesforce org drives both the SOQL and Reports extractors,
so both flavors share the same system.
"""

from __future__ import annotations

from tabsdata_testutils.external import external
from tabsdata_testutils.salesforce import SALESFORCE

from tabsdata._cli.tdconn._internal.extractor.connector.salesforce import (
    report_extractor,
    soql_extractor,
)

SalesforceReportsConnectionExtractor = (
    report_extractor.SalesforceReportsConnectionExtractor
)
SalesforceSOQLConnectionExtractor = soql_extractor.SalesforceSOQLConnectionExtractor


def _info(envs: dict) -> dict:
    """Build a SalesforceConn-shaped info dict from the system env vars."""
    credentials = {
        "username": envs.get("USERNAME"),
        "password": envs.get("PASSWORD"),
        "security_token": envs.get("SECURITY_TOKEN"),
    }
    info = {
        "credentials": {k: v for k, v in credentials.items() if v},
        "instance_url": envs.get("INSTANCE_URL"),
    }
    return {k: v for k, v in info.items() if v}


@external(system=SALESFORCE)
def test_salesforce_soql_check_connect(envs: dict):
    SalesforceSOQLConnectionExtractor().check_connect(_info(envs))


@external(system=SALESFORCE)
def test_salesforce_soql_metadata(envs: dict):
    ext = SalesforceSOQLConnectionExtractor()
    info = _info(envs)
    info["objects"] = ["Account"]
    assert any(e.object == "Account" for e in ext.metadata(info))


@external(system=SALESFORCE)
def test_salesforce_reports_check_connect(envs: dict):
    SalesforceReportsConnectionExtractor().check_connect(_info(envs))


@external(system=SALESFORCE)
def test_salesforce_reports_metadata(envs: dict):
    assert isinstance(
        SalesforceReportsConnectionExtractor().metadata(_info(envs)),
        list,
    )

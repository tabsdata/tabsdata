#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce source-plugin integration tests using fake context"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.salesforce import (
    SalesforceDomain,
    salesforce_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.sql.plugin_io import slot_frame, work_dir
from testi_tabsdatak.conn.salesforce.backend import (
    ACCOUNT,
    report_columns,
    sobject_rows,
    src_conn,
    tabular_report,
)

# noinspection PyProtectedMember
from tabsdatak.conn.salesforce import (
    SalesforceReportSrc,
    SalesforceSrc,
    _plugin as plugin,
)

pytestmark = pytest.mark.salesforce

LIMIT = 3
BEFORE_ANY_ROW = datetime(2000, 1, 1, tzinfo=timezone.utc)
AFTER_EVERY_ROW = datetime(2999, 1, 1, tzinfo=timezone.utc)


@salesforce_domain("salesforce")
def test_src_plugin_read_in_returns_parquet_files(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    src = SalesforceSrc(
        queries=[f"SELECT Id, Name FROM {ACCOUNT} ORDER BY Id LIMIT {LIMIT}"]
    )
    out = plugin._SalesforceSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert len(out) == 1
    frame = slot_frame(out)
    # An org's Account rows are whatever it holds, so the assertion is on the
    # projection and on the LIMIT reaching Salesforce -- not on the contents.
    assert frame.columns == ["Id", "Name"]
    assert frame.height == min(LIMIT, sobject_rows(domain))


@salesforce_domain("salesforce")
def test_src_plugin_read_in_matching_no_rows_writes_empty_parquet(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    src = SalesforceSrc(
        queries=[f"SELECT Id FROM {ACCOUNT} WHERE Name = 'td-no-such-account'"]
    )
    out = plugin._SalesforceSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain_salesforce), src
    )
    # Unlike the SQL sources, a query matching nothing still writes a file, so the
    # slot is never empty; polars cannot infer a schema from zero rows, hence no
    # columns either.
    assert len(out[0]) == 1
    assert slot_frame(out).shape == (0, 0)


@salesforce_domain("salesforce")
def test_src_plugin_read_in_substitutes_last_modified(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    query = f"SELECT Id FROM {ACCOUNT} WHERE SystemModstamp > $lastModified ORDER BY Id"

    everything = slot_frame(
        plugin._SalesforceSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)),
            src_conn(domain),
            SalesforceSrc(queries=[query], initial_last_modified=BEFORE_ANY_ROW),
        )
    )
    assert everything.height == sobject_rows(domain)

    nothing = slot_frame(
        plugin._SalesforceSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path, "rerun")),
            src_conn(domain),
            SalesforceSrc(queries=[query], initial_last_modified=AFTER_EVERY_ROW),
        )
    )
    assert nothing.height == 0


@salesforce_domain("salesforce")
def test_src_plugin_read_in_include_deleted(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    src = SalesforceSrc(
        queries=[f"SELECT Id FROM {ACCOUNT} ORDER BY Id"], include_deleted=True
    )
    out = plugin._SalesforceSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    # queryAll also scans the recycle bin, so it can only see more than query does.
    assert slot_frame(out).height >= sobject_rows(domain)


@salesforce_domain("salesforce")
def test_report_src_plugin_read_in_returns_one_slot_per_report(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    report = tabular_report(domain)
    # The same report twice: each entry gets its own instance and output slot, which
    # is the cardinality contract -- an org need not hold two Tabular reports.
    src = SalesforceReportSrc(reports=[report, report])
    out = plugin._SalesforceReportSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert len(out) == 2
    expected = report_columns(domain, report)
    assert slot_frame(out, 0).columns == expected
    assert slot_frame(out, 1).columns == expected


@salesforce_domain("salesforce")
def test_report_src_plugin_read_in_label_column_names(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    report = tabular_report(domain)
    src = SalesforceReportSrc(reports=[report], column_name_strategy="label")
    out = plugin._SalesforceReportSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain), src
    )
    assert slot_frame(out).columns == report_columns(domain, report, "label")

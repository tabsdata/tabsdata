#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce source-plugin integration tests using fake context"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.salesforce import (
    SalesforceDomain,
    salesforce_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginSrcCtx
from testi_tabsdatak.conn.common.landed import taken
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
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


def served(out: list, folder: Path) -> list[pl.DataFrame]:
    """Every input of a ``read_in`` landed and served as the server does, its
    tables concatenated."""
    return [pl.concat([table.frame for table in slot]) for slot in taken(out, folder)]


LIMIT = 3
BEFORE_ANY_ROW = datetime(2000, 1, 1, tzinfo=timezone.utc)
AFTER_EVERY_ROW = datetime(2999, 1, 1, tzinfo=timezone.utc)


@salesforce_domain("salesforce")
def test_src_plugin_read_in_returns_the_rows(
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
    [frame] = served(out, tmp_path / "served")
    # An org's Account rows are whatever it holds, so the assertion is on the
    # projection and on the LIMIT reaching Salesforce -- not on the contents.
    assert frame.columns == ["Id", "Name"]
    assert frame.height == min(LIMIT, sobject_rows(domain))


@salesforce_domain("salesforce")
def test_src_plugin_read_in_matching_no_rows_lands_an_empty_table(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    src = SalesforceSrc(
        queries=[f"SELECT Id FROM {ACCOUNT} WHERE Name = 'td-no-such-account'"]
    )
    out = plugin._SalesforceSrcPlugin().read_in(
        PluginSrcCtx(work_dir(tmp_path)), src_conn(domain_salesforce), src
    )
    # Unlike the SQL sources, a query matching nothing still hands over a table,
    # so the input is never empty; with no record to type, it has no columns
    # either.
    assert len(out[0]) == 1
    assert served(out, tmp_path / "served")[0].shape == (0, 0)


@salesforce_domain("salesforce")
def test_src_plugin_read_in_substitutes_last_modified(
    domain_salesforce: SalesforceDomain, tmp_path: Path
):
    domain = domain_salesforce
    query = f"SELECT Id FROM {ACCOUNT} WHERE SystemModstamp > $lastModified ORDER BY Id"

    [everything] = served(
        plugin._SalesforceSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path)),
            src_conn(domain),
            SalesforceSrc(queries=[query], initial_last_modified=BEFORE_ANY_ROW),
        ),
        tmp_path / "served",
    )
    assert everything.height == sobject_rows(domain)

    [nothing] = served(
        plugin._SalesforceSrcPlugin().read_in(
            PluginSrcCtx(work_dir(tmp_path, "rerun")),
            src_conn(domain),
            SalesforceSrc(queries=[query], initial_last_modified=AFTER_EVERY_ROW),
        ),
        tmp_path / "served-rerun",
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
    assert served(out, tmp_path / "served")[0].height >= sobject_rows(domain)


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
    first, second = served(out, tmp_path / "served")
    assert first.columns == expected
    assert second.columns == expected


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
    [frame] = served(out, tmp_path / "served")
    assert frame.columns == report_columns(domain, report, "label")

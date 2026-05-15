from __future__ import annotations

from tabsdatak.conn.salesforce import (
    SalesforceConn,
    SalesforceReportSrc,
    SalesforceSrc,
)
from tabsdatak.spi import (
    Explorer,
    SrcDef,
    SrcPlugin,
    TableMode,
)


class _SalesforceSrcPlugin(SrcPlugin[SalesforceSrc]): ...


class _SalesforceReportSrcPlugin(SrcPlugin[SalesforceReportSrc]): ...


class _SalesforceExplorer(Explorer[SalesforceConn]): ...


SALESFORCE_SRC = SrcDef(
    conn=SalesforceConn,
    src_type="salesforce",
    src_version="v1",
    src=SalesforceSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_SalesforceSrcPlugin,
    explorer=_SalesforceExplorer,
)


SALESFORCE_REPORT_SRC = SrcDef(
    conn=SalesforceConn,
    src_type="salesforce_report",
    src_version="v1",
    src=SalesforceReportSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.reports),
    plugin=_SalesforceReportSrcPlugin,
    explorer=_SalesforceExplorer,
)

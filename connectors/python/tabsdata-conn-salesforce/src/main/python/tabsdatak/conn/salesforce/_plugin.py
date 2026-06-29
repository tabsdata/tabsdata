from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.salesforce import (
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceSrcConn,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.spi import (
    Explorer,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableMode,
    to_table_file_inputs,
)

logger = logging.getLogger(__name__)


# --- src_cfg keys ---------------------------------------------------
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "salesforce.logging_level"

_SRC_CFGS_DEFAULTS: dict[SrcCfgKey, Any] = {
    _KEY_SRC_LOGGING_LEVEL: logging.ERROR,
}


@dataclass(frozen=True)
class _ReaderCfg:
    logging_level: int


def _resolve_src_cfg(src_cfg: SrcCfgSpec | None) -> _ReaderCfg:
    cfg = src_cfg or {}
    for k in cfg:
        if k not in _SRC_CFGS_DEFAULTS:
            logger.warning(
                f"Unknown src_cfg key {k!r}, ignoring (allowed: "
                f"{sorted(_SRC_CFGS_DEFAULTS)})"
            )

    def _get(key: SrcCfgKey) -> Any:
        return cfg.get(key, _SRC_CFGS_DEFAULTS[key])

    return _ReaderCfg(
        logging_level=int(_get(_KEY_SRC_LOGGING_LEVEL)),
    )


# All simple-salesforce / polars / pandas / ijson imports are made
# inside the plugin method bodies (or in ``_reader.py``) so the
# conn-salesforce package stays cheap to import for registry scans
# that only care about the SrcDef metadata.


class _SalesforceSrcPlugin(SrcPlugin[SalesforceSrcConn, SalesforceSrc]):
    """Run SOQL queries against Salesforce and emit one parquet file
    per query (``TableMode.SINGLE``)."""

    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: SalesforceSrcConn,
        src: SalesforceSrc,
    ) -> list[list[TableFileInput]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.salesforce import _reader

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("salesforce: read_in starting (%d quer(ies))", len(src.queries))

        api_version = src.api_version
        sf = _reader.login(conn, api_version=api_version)
        try:
            return to_table_file_inputs(_reader.read_soql_src(sf, src, ctx.work_dir))
        finally:
            logger.info("salesforce: read_in finished (%d quer(ies))", len(src.queries))


class _SalesforceReportSrcPlugin(SrcPlugin[SalesforceSrcConn, SalesforceReportSrc]):
    """Run Salesforce Analytics reports and emit one parquet file per
    report (``TableMode.SINGLE``)."""

    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: SalesforceSrcConn,
        src: SalesforceReportSrc,
    ) -> list[list[TableFileInput]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.salesforce import _reader

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("salesforce: read_in starting (%d report(s))", len(src.reports))

        sf = _reader.login(conn)
        try:
            return to_table_file_inputs(_reader.read_report_src(sf, src, ctx.work_dir))
        finally:
            logger.info("salesforce: read_in finished (%d report(s))", len(src.reports))


class _SalesforceExplorer(Explorer[SalesforceSrcConn]):
    def metadata(self, conn: SalesforceSrcConn) -> Any:
        # TODO
        pass

    def data(self, conn: SalesforceSrcConn, query: str) -> Any:
        # TODO
        pass


SALESFORCE_SRC = SrcDef(
    conn=SalesforceSrcConn,
    src_type="salesforce",
    src_version="v1",
    src=SalesforceSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_SalesforceSrcPlugin,
    explorer=_SalesforceExplorer,
)


SALESFORCE_REPORT_SRC = SrcDef(
    conn=SalesforceSrcConn,
    src_type="salesforce_report",
    src_version="v1",
    src=SalesforceReportSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.reports),
    plugin=_SalesforceReportSrcPlugin,
    explorer=_SalesforceExplorer,
)

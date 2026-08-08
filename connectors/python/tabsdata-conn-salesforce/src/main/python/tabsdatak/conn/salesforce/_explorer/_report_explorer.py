#
# Copyright 2026 Tabsdata Inc.
#

"""Salesforce Reports metadata explorer."""

import fnmatch
import sys
from typing import ClassVar

from tabsdatak.conn.common._explorer import (
    BaseConnMetaModel,
    Column,
    ReportingConnMetaModel,
    ReportingEntity,
)
from tabsdatak.conn.salesforce import SalesforceSrcConn
from tabsdatak.conn.salesforce._explorer._common import (
    SalesforceReportEntity,
    check_connect as _sf_check_connect,
    connect,
)
from tabsdatak.spi import Entity, Explorer


class SalesforceReportsConnectionExplorer(Explorer):
    """Explore report metadata via the Salesforce Analytics API.

    Lists the org's reports (SOQL over the ``Report`` object) and, for
    each, resolves its columns through the
    ``analytics/reports/<id>/describe`` endpoint. Every entity carries
    the report's folder (as ``workspace``), its name, and its Salesforce
    report ID.
    """

    meta_model: ClassVar[BaseConnMetaModel] = ReportingConnMetaModel()
    entity_type: ClassVar[type[Entity]] = SalesforceReportEntity

    def check_connect(self, conn: SalesforceSrcConn) -> None:
        _sf_check_connect(conn)

    def metadata(self, conn: SalesforceSrcConn) -> list[ReportingEntity]:
        sf = connect(conn)

        # ``FolderName`` isn't queryable on every org, so fall back to a
        # narrower projection when the first query is rejected.
        query = (
            "SELECT Id, Name, DeveloperName, FolderName "
            "FROM Report WHERE IsDeleted = false ORDER BY Name"
        )
        try:
            result = sf.query_all(query)
        except Exception:
            query = (
                "SELECT Id, Name, DeveloperName "
                "FROM Report WHERE IsDeleted = false ORDER BY Name"
            )
            result = sf.query_all(query)

        reports = _match_reports(result.get("records", []), ["*"])

        entities: list[ReportingEntity] = []
        for report in reports:
            report_id = report["Id"]
            report_name = report["Name"]
            folder = report.get("FolderName", "") or ""

            try:
                describe = sf.restful(f"analytics/reports/{report_id}/describe")
            except Exception as e:
                print(
                    f"  Ignoring report {report_id}/{report_name}: "
                    f"cannot describe ({e})",
                    file=sys.stderr,
                )
                continue

            columns = _extract_columns(describe)
            if not columns:
                print(
                    f"  Ignoring report {report_id}/{report_name}: no columns",
                    file=sys.stderr,
                )
                continue

            entities.append(
                SalesforceReportEntity(
                    connection_type=self.meta_model,
                    workspace=folder,
                    report=report_name,
                    report_id=report_id,
                    columns=columns,
                )
            )

        return entities

    def data(
        self, conn: SalesforceSrcConn, entity: SalesforceReportEntity, limit: int = 100
    ) -> list[dict]:
        report_id = entity.report_id
        if not report_id:
            return []

        sf = connect(conn)
        result = sf.restful(
            f"analytics/reports/{report_id}", params={"includeDetails": "true"}
        )

        col_names = [c.name for c in entity.columns]
        rows: list[dict] = []
        # A report's rows are keyed by grouping in ``factMap`` (tabular
        # reports use the single ``"T!T"`` key); flatten them all.
        for group in result.get("factMap", {}).values():
            for row_data in group.get("rows", []):
                cells = row_data.get("dataCells", [])
                row = {
                    col_names[i]: cell.get("label", cell.get("value", ""))
                    for i, cell in enumerate(cells)
                    if i < len(col_names)
                }
                rows.append(row)
                if len(rows) >= limit:
                    return rows

        return rows


def _match_reports(reports: list[dict], patterns: list[str]) -> list[dict]:
    """Return the reports whose name matches any glob pattern (case-insensitive),
    each at most once."""
    matched = []
    seen = set()
    for pattern in patterns:
        for report in reports:
            rid = report["Id"]
            name = report.get("Name", "")
            if rid not in seen and fnmatch.fnmatch(name.lower(), pattern.lower()):
                matched.append(report)
                seen.add(rid)
    return matched


def _extract_columns(describe: dict) -> list[Column]:
    """Build the column list from a report ``describe`` response.

    Salesforce splits a report's columns across three sections of the
    extended metadata -- detail, grouping, and aggregate -- so all three
    are collected here.
    """
    report_meta = describe.get("reportMetadata", {})
    extended_meta = describe.get("reportExtendedMetadata", {})
    detail_col_info = extended_meta.get("detailColumnInfo", {})
    group_col_info = extended_meta.get("groupingColumnInfo", {})
    agg_col_info = extended_meta.get("aggregateColumnInfo", {})

    def _column(col_key: str, col_info: dict, description: str = "") -> Column:
        return Column(
            name=col_info.get("label", col_key),
            type=col_info.get("dataType", "STRING").upper(),
            nullable=True,
            primary_key=False,
            description=description,
        )

    columns: list[Column] = []

    # Detail columns, ordered by ``reportMetadata.detailColumns`` when
    # present; otherwise fall back to the info map's own order.
    detail_columns = report_meta.get("detailColumns") or []
    if detail_columns:
        columns.extend(
            _column(key, detail_col_info.get(key, {})) for key in detail_columns
        )
    else:
        columns.extend(_column(key, info) for key, info in detail_col_info.items())

    columns.extend(
        _column(key, info, "grouping") for key, info in group_col_info.items()
    )
    columns.extend(
        _column(key, info, "aggregate") for key, info in agg_col_info.items()
    )

    return columns

#
# Copyright (c) 2026 Tabsdata Inc.
#

"""Salesforce Reports metadata extractor."""

import fnmatch
import sys
from dataclasses import dataclass
from typing import ClassVar

from tabsdata._cli.tdconn._internal.extractor.api import (
    Column,
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    ReportingConnMetaModel,
    ReportingEntity,
)
from tabsdata._cli.tdconn._internal.extractor.connector.salesforce.common import (
    check_connect as _sf_check_connect,
    connect,
)


class SalesforceReportsConnectionExtractor(ConnectionExtractor):
    """Extract report metadata from Salesforce Analytics API.

    The ``info`` dict matches ``SalesforceConn``'s JSON shape:
        credentials: {username, password, security_token}
        instance_url: optional Salesforce instance URL

    Plus one extractor-level option:
        reports: list of report name patterns to extract, supports * wildcard
                 e.g. ["Pipeline*", "Q* Revenue"]
                 Use ["*"] for all reports.

    Each extracted entity includes both the report ID and name.
    """

    meta_model: ClassVar[ConnMetaModel] = ReportingConnMetaModel()
    # ``entity_type`` is forward-referenced so the extractor class and
    # its SalesforceReportEntity subclass can live in the same module
    # without circularity; assigned just below the class definition.

    def check_connect(self, info: dict) -> None:
        _sf_check_connect(info)

    def metadata(self, info: dict) -> list[ReportingEntity]:
        sf = connect(info)
        report_patterns = info.get("reports", ["*"])

        # List all reports via SOQL
        query = (
            "SELECT Id, Name, DeveloperName, FolderName "
            "FROM Report "
            "WHERE IsDeleted = false "
            "ORDER BY Name"
        )
        try:
            result = sf.query_all(query)
        except Exception:
            # FolderName may not be available — try without it
            query = (
                "SELECT Id, Name, DeveloperName "
                "FROM Report "
                "WHERE IsDeleted = false "
                "ORDER BY Name"
            )
            result = sf.query_all(query)

        all_reports = result.get("records", [])

        # Filter by name patterns
        selected = _match_reports(all_reports, report_patterns)

        entities = []
        for report in selected:
            report_id = report["Id"]
            report_name = report["Name"]
            folder = report.get("FolderName", "") or ""

            # Describe the report to get column metadata
            try:
                describe = sf.restful(f"analytics/reports/{report_id}/describe")
            except Exception as e:
                print(
                    f"  Ignoring report: {report_id}/{report_name} "
                    f"reason: cannot describe ({e})",
                    file=sys.stderr,
                )
                continue

            columns = _extract_columns(describe)

            if not columns:
                print(
                    f"  Ignoring report: {report_id}/{report_name} reason: no columns",
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

    def data(self, info: dict, entity: Entity, limit: int = 100) -> list[dict]:
        sf = connect(info)
        report_entity = entity  # type: SalesforceReportEntity
        report_id = report_entity.report_id
        if not report_id:
            return []

        # Run the report to get data
        result = sf.restful(
            f"analytics/reports/{report_id}", params={"includeDetails": "true"}
        )

        rows = []
        fact_map = result.get("factMap", {})
        # Tabular reports have a "T!T" key
        for key, group in fact_map.items():
            for row_data in group.get("rows", []):
                cells = row_data.get("dataCells", [])
                row = {}
                col_names = [c.name for c in report_entity.columns]
                for i, cell in enumerate(cells):
                    if i < len(col_names):
                        row[col_names[i]] = cell.get("label", cell.get("value", ""))
                rows.append(row)
                if len(rows) >= limit:
                    break
            if len(rows) >= limit:
                break

        return rows


@dataclass
class SalesforceReportEntity(ReportingEntity):
    """ReportingEntity that includes the Salesforce report ID."""

    report_id: str = ""

    def to_dict(self) -> dict:
        d = super().to_dict()
        if self.report_id:
            d["report_id"] = self.report_id
        return d

    @classmethod
    def from_dict(cls, data: dict, connection_type) -> "SalesforceReportEntity":
        base = ReportingEntity.from_dict(data, connection_type)
        return cls(
            connection_type=base.connection_type,
            workspace=base.workspace,
            report=base.report,
            columns=base.columns,
            report_id=data.get("report_id", ""),
        )


# Wire up entity_type now that SalesforceReportEntity is defined.
SalesforceReportsConnectionExtractor.entity_type = SalesforceReportEntity


def _match_reports(reports: list[dict], patterns: list[str]) -> list[dict]:
    """Filter reports by name glob patterns (case-insensitive)."""
    matched = []
    seen = set()
    for pattern in patterns:
        for report in reports:
            name = report.get("Name", "")
            rid = report["Id"]
            if rid not in seen and fnmatch.fnmatch(name.lower(), pattern.lower()):
                matched.append(report)
                seen.add(rid)
    return matched


def _extract_columns(describe: dict) -> list[Column]:
    """Extract column metadata from a report describe response."""
    columns = []

    # Try reportMetadata.detailColumns for column order
    report_meta = describe.get("reportMetadata", {})
    extended_meta = describe.get("reportExtendedMetadata", {})
    detail_col_info = extended_meta.get("detailColumnInfo", {})
    group_col_info = extended_meta.get("groupingColumnInfo", {})
    agg_col_info = extended_meta.get("aggregateColumnInfo", {})

    # Detail columns (the main report columns)
    detail_columns = report_meta.get("detailColumns") or []
    for col_key in detail_columns:
        col_info = detail_col_info.get(col_key, {})
        columns.append(
            Column(
                name=col_info.get("label", col_key),
                type=col_info.get("dataType", "STRING").upper(),
                nullable=True,
                primary_key=False,
                description="",
            )
        )

    # If no detail columns from reportMetadata, iterate detailColumnInfo directly
    if not columns and detail_col_info:
        for col_key, col_info in detail_col_info.items():
            columns.append(
                Column(
                    name=col_info.get("label", col_key),
                    type=col_info.get("dataType", "STRING").upper(),
                    nullable=True,
                    primary_key=False,
                    description="",
                )
            )

    # Grouping columns
    for col_key, col_info in group_col_info.items():
        columns.append(
            Column(
                name=col_info.get("label", col_key),
                type=col_info.get("dataType", "STRING").upper(),
                nullable=True,
                primary_key=False,
                description="grouping",
            )
        )

    # Aggregate columns
    for col_key, col_info in agg_col_info.items():
        columns.append(
            Column(
                name=col_info.get("label", col_key),
                type=col_info.get("dataType", "STRING").upper(),
                nullable=True,
                primary_key=False,
                description="aggregate",
            )
        )

    return columns

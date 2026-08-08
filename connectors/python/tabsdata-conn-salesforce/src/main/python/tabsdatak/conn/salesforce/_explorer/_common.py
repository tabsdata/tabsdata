#
# Copyright 2026 Tabsdata Inc.
#

from simple_salesforce import Salesforce

import tabsdatak._api as _api
from tabsdatak.conn.common._explorer import ReportingEntity
from tabsdatak.conn.salesforce import SalesforceSrcConn
from tabsdatak.conn.salesforce.error import SalesforceErrorCode


def connect(conn: SalesforceSrcConn) -> Salesforce:
    """Create a Salesforce client from a SalesforceConn object."""
    return Salesforce(
        username=conn.credentials.username.value(),
        password=conn.credentials.password.value(),
        security_token=conn.credentials.security_token.value(),
        instance_url=conn._instance_url() if conn.instance_url is not None else None,
    )


def check_connect(conn: SalesforceSrcConn) -> None:
    """Verify Salesforce connectivity by fetching instance limits.

    ``simple_salesforce``'s ``Salesforce(...)`` constructor already
    performs the OAuth login, so the constructor itself raises on bad
    credentials; the follow-up ``limits()`` call confirms the session
    token works against the REST API.
    """
    connect(conn).limits()


@_api.dataclass(SalesforceErrorCode.SALESFORCE_18)
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

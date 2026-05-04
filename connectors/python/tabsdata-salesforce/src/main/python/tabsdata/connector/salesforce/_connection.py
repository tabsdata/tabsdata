#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._io.connections.connections import Conn
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.salesforce._connector import (
    SalesforceCredentials,
    SalesforceTokenCredentials,
)
from tabsdata.exceptions import ErrorCode, ConnectionConfigurationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SalesforceConn(Conn):
    """Salesforce connection config. Required: credentials (SalesforceTokenCredentials).
    Connection configuration for Salesforce.
    """

    @_td_validate_call(ErrorCode.DECE101)
    def __init__(
        self,
        credentials: SalesforceCredentials,
        instance_url: str | None = None,
        category: str | None = None,
        objects: str|list[str] | None = None,
    ):
        """
        Initialize a SalesforceConn instance. The connection will support
        the objects of the given category or in the object list.

        Args:
            credentials: Credentials for authenticating with Salesforce.
            instance_url: Optional Salesforce instance URL. Required only when
                the username/password are associated with more than one instance.
            category: a category of objects (refer to the `CATEGORIES` constant
                to see the available categories and the objects in each category.
                If set, `objects` cannot be set.
            objects: ad hoc group of objects. If set, `category` cannot be set.
        """
        self.credentials = credentials
        self.instance_url = instance_url
        if category and objects:
            raise ConnectionConfigurationError(ErrorCode.CXCE11)
        self.category = category
        self.objects = objects

    @property
    def credentials(self) -> SalesforceCredentials:
        """
        The credentials for authenticating with Salesforce.
        """
        return self._credentials

    @credentials.setter
    @_td_validate_call(ErrorCode.DECE101)
    def credentials(self, credentials: SalesforceCredentials):
        self._credentials = credentials

    @property
    def instance_url(self) -> str | None:
        """
        The Salesforce instance URL.
        """
        return self._instance_url

    @instance_url.setter
    @_td_validate_call(ErrorCode.DECE101)
    def instance_url(self, instance_url: str | None):
        self._instance_url = instance_url

    @property
    def category(self) -> str | None:
        return self._category


    @category.setter
    @_td_validate_call(ErrorCode.DECE101)
    def category(self, category: str | None):
        if category is not None and category not in CATEGORIES:
            raise ConnectionConfigurationError(
                ErrorCode.CXCE13, category, sorted(CATEGORIES.keys())
            )
        self._category = category


    @property
    def objects(self) -> str | list[str] | None:
        if self._category is not None:
            return CATEGORIES[self._category]
        return self._objects


    @objects.setter
    @_td_validate_call(ErrorCode.DECE101)
    def objects(self, objects: str | list[str] | None):
        self._objects = objects

    @classmethod
    def _load_from_dict(cls, configs: dict) -> "SalesforceConn":
        credentials = configs.get("credentials")
        if isinstance(credentials, dict):
            configs = dict(configs)
            configs["credentials"] = SalesforceTokenCredentials(**credentials)
        return cls(**configs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SalesforceConn):
            return False
        return (
            self.credentials == other.credentials
            and self.instance_url == other.instance_url
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"credentials={self.credentials}, "
            f"instance_url={self.instance_url!r})"
        )

CATEGORIES = {
    "sales_crm": [
        "Account",
        "Contact",
        "Lead",
        "Opportunity",
        "OpportunityLineItem",
        "OpportunityContactRole",
        "OpportunityHistory",
        "OpportunityFieldHistory",
        "Campaign",
        "CampaignMember",
        "CampaignMemberStatus"
    ],
    "quote_to_cash": [
        "Quote",
        "QuoteLineItem",
        "QuoteDocument",
        "Contract",
        "ContractHistory",
        "Order",
        "OrderItem",
        "Pricebook2",
        "PricebookEntry",
        "Product2"
    ],
    "service": [
        "Case",
        "CaseComment",
        "CaseHistory",
        "CaseContactRole",
        "CaseSolution",
        "CaseTeamMember",
        "Solution",
        "Entitlement",
        "ServiceContract",
        "ContractLineItem",
        "Incident"
    ],
    "activity": [
        "Task",
        "Event",
        "EventRelation",
        "TaskRelation",
        "EventWhoRelation",
        "TaskWhoRelation",
        "AcceptedEventRelation",
        "DeclinedEventRelation",
        "UndecidedEventRelation",
        "RecurringEvent",
        "TaskFeed",
        "EventFeed",
        "TaskPriority",
        "TaskStatus",
        "TaskType",
        "OpenActivity",
        "ActivityHistory"
    ],
    "user_org": [
        "User",
        "UserRole",
        "Profile",
        "PermissionSet",
        "PermissionSetAssignment",
        "PermissionSetGroup",
        "Group",
        "GroupMember",
        "Organization",
        "BusinessHours",
        "Holiday"
    ],
    "territory": [
        "Territory2",
        "Territory2Model",
        "UserTerritory2Association",
        "AccountTerritoryAssignmentRule"
    ],
    "content": [
        "ContentDocument",
        "ContentVersion",
        "ContentDocumentLink",
        "Attachment",
        "Note",
        "Document"
    ],
    "communication": [
        "EmailMessage",
        "EmailTemplate",
        "EmailMessageRelation"
    ]
}

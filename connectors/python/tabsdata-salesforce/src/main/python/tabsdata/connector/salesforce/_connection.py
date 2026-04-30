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
from tabsdata.exceptions import ErrorCode

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
    ):
        """
        Initialize a SalesforceConn instance.

        Args:
            credentials: Credentials for authenticating with Salesforce.
            instance_url: Optional Salesforce instance URL. Required only when
                the username/password are associated with more than one instance.
        """
        self.credentials = credentials
        self.instance_url = instance_url

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

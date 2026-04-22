#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.plugin import SourcePlugin
from tabsdata._utils.validation import _td_validate_call
from tabsdata.connector.salesforce._connection import SalesforceConn
from tabsdata.connector.salesforce._connector import (
    SalesforceSource,
)
from tabsdata.exceptions import ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class SalesforceSrc(SourcePlugin):
    """
    Source plugin for reading data from Salesforce using SOQL queries.
    """

    @_td_validate_call(ErrorCode.SOCE102)
    def __init__(
        self,
        conn: SalesforceConn | NamedConn,
        query: str | list[str],
        include_deleted: bool = False,
        initial_last_modified: str | None = None,
        api_version: str | None = None,
    ):
        """
        Initialize a SalesforceSrc instance.

        Args:
            conn: The Salesforce connection configuration.
            query: The SOQL query or list of queries to execute.
            include_deleted: Whether to include deleted records in the query
                results. Defaults to False.
            initial_last_modified: Optional initial last-modified timestamp
                (Salesforce datetime format, with timezone) used to replace
                the $lastModified token in queries.
            api_version: Optional Salesforce API version to use.
        """
        # Initialize all non-conn state first, so the conn setter (which
        # rebuilds the inner SalesforceSource) has everything it needs.
        self._include_deleted = include_deleted
        self._initial_last_modified = initial_last_modified
        self._api_version = api_version
        self._query_input = query
        self._salesforce_source = None
        # Assigning conn triggers the setter, which will (re)build the inner
        # SalesforceSource if conn is a resolved SalesforceConn.
        self.conn = conn

    def _build_source(self) -> SalesforceSource:
        kwargs = {}
        if self._api_version is not None:
            kwargs["api_version"] = self._api_version
        return SalesforceSource(
            credentials=self._conn.credentials,
            query=self._query_input,
            instance_url=self._conn.instance_url,
            include_deleted=self._include_deleted,
            initial_last_modified=self._initial_last_modified,
            **kwargs,
        )

    @property
    def conn(self) -> SalesforceConn | NamedConn:
        """
        The Salesforce connection configuration.
        """
        return self._conn

    @conn.setter
    @_td_validate_call(ErrorCode.SOCE102)
    def conn(self, conn: SalesforceConn | NamedConn):
        self._conn = conn
        # Rebuild the inner SalesforceSource so its credentials and
        # instance_url reflect the new conn. When conn is still a NamedConn
        # (not yet resolved by the framework), skip rebuilding since we
        # don't have real credentials yet.
        if isinstance(conn, SalesforceConn):
            self._salesforce_source = self._build_source()
        else:
            self._salesforce_source = None

    @property
    def query(self) -> list[str]:
        """
        The SOQL queries to execute.
        """
        if self._salesforce_source is not None:
            return self._salesforce_source.query
        # Fall back to local state when conn hasn't been resolved yet.
        return (
            [self._query_input]
            if isinstance(self._query_input, str)
            else self._query_input
        )

    @property
    def include_deleted(self) -> bool:
        """
        Whether deleted records are included in the query results.
        """
        if self._salesforce_source is not None:
            return self._salesforce_source.include_deleted
        return self._include_deleted

    @property
    def initial_last_modified(self) -> str | None:
        """
        The initial last-modified timestamp used for $lastModified token
        replacement in queries.
        """
        return self._initial_last_modified

    @property
    def api_version(self) -> str | None:
        """
        The Salesforce API version being used.
        """
        if self._salesforce_source is not None:
            return self._salesforce_source.api_version
        return self._api_version

    @property
    def initial_values(self) -> dict:
        """
        The initial values used to resolve query tokens, and to track offset
        state across runs.
        """
        if self._salesforce_source is not None:
            return self._salesforce_source.initial_values
        return {}

    @initial_values.setter
    def initial_values(self, initial_values: dict | None):
        if self._salesforce_source is None:
            raise RuntimeError(
                "Cannot set initial_values before the conn has been resolved "
                "to a SalesforceConn."
            )
        self._salesforce_source.initial_values = initial_values or {}

    def chunk(self, working_dir: str) -> list[str]:
        """
        Execute the configured queries and return a list of parquet file
        paths (relative to ``working_dir``) with the results.
        """
        if self._salesforce_source is None:
            raise RuntimeError(
                "Cannot call chunk() before the conn has been resolved to a "
                "SalesforceConn."
            )
        return self._salesforce_source.chunk(working_dir)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"conn={self.conn}, "
            f"query={self.query}, "
            f"include_deleted={self.include_deleted}, "
            f"initial_last_modified={self.initial_last_modified!r}, "
            f"api_version={self.api_version!r})"
        )
